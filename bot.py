"""
XTEN: арбитраж расхождения цены Hyperliquid <-> Lighter.

Направления:
  A (+1): HL дороже  -> шорт HL, лонг Lighter
  B (-1): HL дешевле -> лонг HL, шорт Lighter

Вход: исполнимый спред (VWAP на размер клипа) >= entry_spread_pct, держится confirm_sec.
Добор: каждый следующий клип при спреде >= entry + add_step * n, до max_position_usd на ногу.
Выход: ступенями exit_levels по схождению (mid или exec).
Риск: ликвидационная дистанция на каждой ноге -> довнести маржу / сократить / закрыть всё.
Ноги: после каждой сделки позиции сверяются; дисбаланс добивается или откатывается.

Запуск:  python bot.py paper   |   python bot.py live
"""
from __future__ import annotations

import asyncio
import csv
import html
import json
import logging
import signal
import sys
import time
from pathlib import Path

import config as config_mod
from books import Book, HyperliquidFeed, LighterFeed
from tg import Telegram
from venues import HyperliquidVenue, LighterVenue, PaperVenue, Snapshot, BaseVenue

ROOT = Path(__file__).resolve().parent
LOGS = ROOT / "logs"
STATE_FILE = ROOT / "state.json"
STOP_FILE = ROOT / "STOP"

log = logging.getLogger("arb")

DIR_NAME = {1: "A: шорт HL / лонг Lighter", -1: "B: лонг HL / шорт Lighter", 0: "нет позиции"}


def fmt(x, nd=4):
    return "—" if x is None else f"{x:,.{nd}f}".replace(",", " ")


class CsvLog:
    def __init__(self, path: Path, header: list[str]):
        self.path, self.header = path, header
        path.parent.mkdir(exist_ok=True)
        if not path.exists():
            with path.open("w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(header)

    def write(self, *row):
        with self.path.open("a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([time.strftime("%Y-%m-%d %H:%M:%S")] + list(row))


class Arb:
    def __init__(self, cfg: config_mod.Config, hl: BaseVenue, lt: BaseVenue,
                 hl_book: Book, lt_book: Book, tg: Telegram | None = None):
        self.cfg, self.hl, self.lt = cfg, hl, lt
        self.hl_book, self.lt_book = hl_book, lt_book
        self.tg = tg or Telegram("", [], self.on_command)
        self.tg.handler = self.on_command
        self.tg.paused_getter = lambda: self.paused or self.manual_hold
        self.lock = asyncio.Lock()
        # состояние
        self.direction = 0
        self.clips = 0
        self.exit_stage = 0
        self.opened_at = 0.0
        self.paused = False
        self.manual_hold = False        # после ручного закрытия одной ноги: бот не трогает позиции сам
        self.snap_hl = Snapshot()
        self.snap_lt = Snapshot()
        self.start_equity = None
        self.last_trade = 0.0
        self.last_risk_action = 0.0
        self._cond_since: dict[str, float] = {}
        self._last_status = time.time()
        self._last_spread_log = 0.0
        self._last_stale_alert = 0.0
        self.running = True
        self.trades = CsvLog(LOGS / "trades.csv", ["ts", "action", "venue", "side", "req_size", "filled",
                                                    "avg_px", "limit_px", "reduce_only", "error"])
        self.spreads = CsvLog(LOGS / "spreads.csv", ["ts", "hl_bid", "hl_ask", "lt_bid", "lt_ask",
                                                      "mid_spread_pct", "a_open_pct", "b_open_pct",
                                                      "hl_pos", "lt_pos"])

    # ------------------------------------------------------------------ состояние на диске
    def save_state(self):
        STATE_FILE.write_text(json.dumps(dict(direction=self.direction, clips=self.clips,
                                              exit_stage=self.exit_stage, opened_at=self.opened_at,
                                              paused=self.paused, manual_hold=self.manual_hold,
                                              start_equity=self.start_equity), indent=2))

    def load_state(self):
        if STATE_FILE.exists():
            try:
                d = json.loads(STATE_FILE.read_text())
                for k in ("clips", "exit_stage", "opened_at", "paused", "manual_hold", "start_equity"):
                    if k in d:
                        setattr(self, k, d[k])
            except Exception as e:  # noqa: BLE001
                log.warning("state.json не прочитан: %s", e)

    # ------------------------------------------------------------------ цены и спреды
    def books_ok(self) -> bool:
        return self.hl_book.fresh(self.cfg.book_stale_sec) and self.lt_book.fresh(self.cfg.book_stale_sec)

    def mid_spread(self) -> float | None:
        """(HL / Lighter - 1) в %, по средним ценам."""
        h, l = self.hl_book.mid(), self.lt_book.mid()
        return (h / l - 1) * 100 if h and l else None

    def exec_spreads(self, size: float):
        """Исполнимые спреды на размер size (в %), None если глубины не хватает.
        a_open: продать HL по бидам / купить Lighter по аскам
        b_open: купить HL по аскам / продать Lighter по бидам
        a_close / b_close: сколько ещё «против нас» при закрытии соответствующего направления"""
        r = {}
        hb = self.hl_book.sweep(False, size)
        ha = self.hl_book.sweep(True, size)
        lb = self.lt_book.sweep(False, size)
        la = self.lt_book.sweep(True, size)
        ok = lambda s: s[0] is not None and s[2] >= size * 0.999  # noqa: E731
        r["a_open"] = (hb[0] / la[0] - 1) * 100 if ok(hb) and ok(la) else None
        r["b_open"] = (lb[0] / ha[0] - 1) * 100 if ok(lb) and ok(ha) else None
        r["a_close"] = (ha[0] / lb[0] - 1) * 100 if ok(ha) and ok(lb) else None
        r["b_close"] = (la[0] / hb[0] - 1) * 100 if ok(la) and ok(hb) else None
        r["sweeps"] = dict(hb=hb, ha=ha, lb=lb, la=la)
        return r

    def divergence_for(self, d: int, size: float) -> float | None:
        """Текущий спред в терминах открытого направления (сколько ещё не сошлось), %."""
        if self.cfg.exit_price == "mid":
            m = self.mid_spread()
            return None if m is None else m * d
        ex = self.exec_spreads(size)
        return ex["a_close"] if d == 1 else ex["b_close"]

    def step(self) -> float:
        return max(self.hl.size_step, self.lt.size_step)

    def floor_common(self, size: float) -> float:
        return self.lt.floor_size(self.hl.floor_size(size)) if self.hl.size_step >= self.lt.size_step \
            else self.hl.floor_size(self.lt.floor_size(size))

    def _held(self, key: str, cond: bool) -> bool:
        """Условие должно продержаться confirm_sec, чтобы не реагировать на один выброс."""
        if not cond:
            self._cond_since.pop(key, None)
            return False
        t0 = self._cond_since.setdefault(key, time.time())
        return time.time() - t0 >= self.cfg.confirm_sec

    # ------------------------------------------------------------------ исполнение
    async def _ioc(self, action: str, v: BaseVenue, is_buy: bool, size: float, limit_px: float,
                   reduce_only: bool):
        f = await v.ioc(is_buy, size, limit_px, reduce_only)
        self.trades.write(action, v.name, "BUY" if is_buy else "SELL", size, f.filled,
                          f.avg_px, limit_px, reduce_only, f.error)
        if f.error:
            log.warning("%s %s %s %.6g @%.6g: %s", action, v.name, "BUY" if is_buy else "SELL",
                        size, limit_px, f.error)
        else:
            log.info("%s %s %s %.6g -> filled %.6g avg %.6g", action, v.name,
                     "BUY" if is_buy else "SELL", size, f.filled, f.avg_px)
        return f

    def _limit(self, book: Book, is_buy: bool, size: float, slip_pct: float) -> float | None:
        _, worst, got = book.sweep(is_buy, size)
        ref = worst if worst else (book.best_ask() if is_buy else book.best_bid())
        if not ref:
            ref = book.mid()
        if not ref:
            return None
        return ref * (1 + slip_pct / 100) if is_buy else ref * (1 - slip_pct / 100)

    async def refresh(self):
        self.snap_hl, self.snap_lt = await asyncio.gather(self.hl.fetch(), self.lt.fetch())
        return self.snap_hl, self.snap_lt

    async def open_clip(self, d: int, size: float, spread: float):
        """d=+1: продать HL, купить Lighter. d=-1: наоборот."""
        hl_buy = d == -1
        hl_px = self._limit(self.hl_book, hl_buy, size, self.cfg.max_slippage_pct)
        lt_px = self._limit(self.lt_book, not hl_buy, size, self.cfg.max_slippage_pct)
        before_hl, before_lt = self.snap_hl.size, self.snap_lt.size
        fh, fl = await asyncio.gather(
            self._ioc("open", self.hl, hl_buy, size, hl_px, False),
            self._ioc("open", self.lt, not hl_buy, size, lt_px, False))
        await self.refresh()
        await self.repair_legs(prefer_complete=True, context="вход")
        dh = self.snap_hl.size - before_hl
        dl = self.snap_lt.size - before_lt
        if abs(dh) < self.step() and abs(dl) < self.step():
            await self.tg.send(f"⚠️ Вход {DIR_NAME[d]} не исполнился: спред ушёл или нет ликвидности.\n"
                               f"HL: {html.escape(fh.error or 'ok')}\nLighter: {html.escape(fl.error or 'ok')}",
                               buttons=False, dedupe_sec=300)
            return
        if self.direction == 0:
            self.opened_at = time.time()
        self.direction = d
        self.clips += 1
        self.exit_stage = 0
        self.save_state()
        await self.tg.send(
            f"🟢 <b>Вход #{self.clips}</b> {DIR_NAME[d]}\n"
            f"Спред на входе: <b>{spread:.2f}%</b>\n"
            f"HL: {'BUY' if hl_buy else 'SELL'} {fmt(abs(dh), 4)} @ {fmt(fh.avg_px, 5)}\n"
            f"Lighter: {'SELL' if hl_buy else 'BUY'} {fmt(abs(dl), 4)} (лимит {fmt(lt_px, 5)})\n\n"
            + self.positions_text())

    async def close_spread(self, fraction: float, reason: str, slip_pct: float | None = None):
        """Сократить обе ноги на долю fraction (1.0 = закрыть полностью), reduce-only."""
        slip = slip_pct if slip_pct is not None else self.cfg.max_slippage_pct
        await self.refresh()
        # частичное закрытие: один общий объём на обе ноги, кратный шагу обеих бирж
        common = self.floor_common(min(abs(self.snap_hl.size), abs(self.snap_lt.size)) * fraction)
        for attempt in range(3):
            tasks = []
            for v, snap, book in ((self.hl, self.snap_hl, self.hl_book),
                                  (self.lt, self.snap_lt, self.lt_book)):
                if not snap.size:
                    continue
                qty = abs(snap.size) if fraction >= 1 else common
                if qty <= 0:
                    continue
                is_buy = snap.size < 0
                px = self._limit(book, is_buy, qty, slip + attempt * 1.0)
                if px is None:
                    continue
                tasks.append(self._ioc("close", v, is_buy, qty, px, True))
            if not tasks:
                break
            await asyncio.gather(*tasks)
            await self.refresh()
            if fraction < 1:
                break
            if not self.snap_hl.size and not self.snap_lt.size:
                break
        if fraction < 1:
            await self.repair_legs(prefer_complete=False, context="сокращение")
        flat = not self.snap_hl.size and not self.snap_lt.size
        if flat:
            self.direction, self.clips, self.exit_stage, self.opened_at = 0, 0, 0, 0.0
        self.save_state()
        await self.tg.send(
            f"{'🔵' if flat else '🟡'} <b>{'Спред закрыт' if flat else ('Спред закрыт не полностью' if fraction >= 1 else f'Спред сокращён на {fraction*100:.0f}%')}</b>\n"
            f"Причина: {html.escape(reason)}\n"
            f"Спред сейчас: {fmt(self.mid_spread(), 2)}%\n\n" + self.positions_text())
        if fraction >= 1 and not flat:
            await self.tg.send("🚨 <b>Не удалось закрыть полностью</b> — проверь стаканы и закрой вручную кнопками.")

    async def close_leg(self, v: BaseVenue, book: Book) -> str:
        for attempt in range(3):
            snap = await v.fetch()
            if not snap.size:
                break
            is_buy = snap.size < 0
            px = self._limit(book, is_buy, abs(snap.size), self.cfg.repair_slippage_pct + attempt)
            if px is None:
                return f"{v.name}: нет цены в стакане"
            await self._ioc("manual_close", v, is_buy, abs(snap.size), px, True)
        await self.refresh()
        left = (self.snap_hl if v is self.hl else self.snap_lt).size
        return f"{v.name}: {'закрыто' if not left else f'осталось {fmt(left, 4)} — повтори'}"

    async def repair_legs(self, prefer_complete: bool, context: str):
        """Выравнивает ноги так, чтобы hl + lighter = 0.
        prefer_complete: сначала дотянуть отставшую ногу (сохраняем вход), потом откатить лишнее."""
        for attempt in range(3):
            net = self.snap_hl.size + self.snap_lt.size
            mid = self.hl_book.mid() or self.lt_book.mid() or 0
            if abs(net) < self.step() * 0.5:
                return True
            big, small = ((self.hl, self.hl_book, self.snap_hl), (self.lt, self.lt_book, self.snap_lt))
            if abs(self.snap_lt.size) > abs(self.snap_hl.size):
                big, small = small, big
            complete = prefer_complete and attempt == 0
            v, book, snap = small if complete else big
            reduce_only = not complete
            is_buy = net < 0           # нетто короткие -> докупаем
            qty = v.floor_size(abs(net))
            if not v.can_trade(qty, mid, reduce_only):
                if attempt == 0:
                    await self.tg.send(
                        f"⚠️ Дисбаланс ног {fmt(net, 4)} XTEN (${abs(net) * mid:.2f}) после «{context}» "
                        f"меньше минимального ордера {v.name} — оставляю.", buttons=False, dedupe_sec=600)
                return False
            px = self._limit(book, is_buy, qty, self.cfg.repair_slippage_pct)
            if px is None:
                return False
            await self._ioc("repair", v, is_buy, qty, px, reduce_only)
            await self.refresh()
            await self.tg.send(
                f"🔧 Выравнивание ног после «{context}»: {v.name} {'BUY' if is_buy else 'SELL'} "
                f"{fmt(qty, 4)} ({'добор' if complete else 'откат лишнего'})\n" + self.positions_text(),
                buttons=False)
        net = self.snap_hl.size + self.snap_lt.size
        if abs(net) >= self.step():
            self.paused = True
            self.save_state()
            await self.tg.send(f"🚨 <b>Ноги не выровнены</b>: нетто {fmt(net, 4)} XTEN. "
                               f"Новые входы на паузе. Разберись вручную.")
            return False
        return True

    # ------------------------------------------------------------------ стратегия
    async def strategy_tick(self):
        if not self.books_ok():
            if self.direction and time.time() - self._last_stale_alert > 300:
                self._last_stale_alert = time.time()
                await self.tg.send("⚠️ Стакан одной из бирж устарел — торговля приостановлена до восстановления.",
                                   buttons=False)
            return
        mid = (self.hl_book.mid() + self.lt_book.mid()) / 2
        clip = self.floor_common(self.cfg.clip_usd / mid)
        ex = self.exec_spreads(clip) if clip > 0 else {}
        self._log_spread(ex)
        if self.manual_hold or time.time() - self.last_trade < self.cfg.min_trade_interval_sec:
            return

        # ---- выходы и стопы
        if self.direction:
            pos_size = min(abs(self.snap_hl.size), abs(self.snap_lt.size)) or clip
            div = self.divergence_for(self.direction, pos_size)
            if div is not None:
                if self.cfg.stop_spread_pct and self._held("stop", div >= self.cfg.stop_spread_pct):
                    await self._trade(self.close_spread(1.0, f"стоп: спред разошёлся до {div:.2f}%",
                                                        self.cfg.repair_slippage_pct))
                    self.paused = True
                    return
                if self.cfg.max_hold_hours and self.opened_at and \
                        time.time() - self.opened_at > self.cfg.max_hold_hours * 3600:
                    await self._trade(self.close_spread(1.0, f"держим дольше {self.cfg.max_hold_hours} ч"))
                    return
                if self.exit_stage < len(self.cfg.exit_levels):
                    thr, frac = self.cfg.exit_levels[self.exit_stage]
                    if self._held(f"exit{self.exit_stage}", div <= thr):
                        self.exit_stage += 1
                        await self._trade(self.close_spread(frac, f"схождение: спред {div:.2f}% ≤ {thr}%"))
                        return

        # ---- входы и доборы
        if self.paused or clip <= 0:
            return
        for d in (1, -1):
            if self.direction not in (0, d):
                continue
            need = self.cfg.entry_spread_pct + self.cfg.add_step_pct * (self.clips if self.direction == d else 0)
            leg_usd = max(abs(self.snap_hl.size), abs(self.snap_lt.size)) * mid
            room = self.floor_common(min(clip, (self.cfg.max_position_usd - leg_usd) / mid))
            size, s = self.fit_size(d, need, room, mid)
            if self._held(f"open{d}", size > 0):
                why = self.margin_block(size * mid)
                if why:
                    await self.tg.send(f"⚠️ Спред есть, но {why} — вход пропущен.",
                                       buttons=False, dedupe_sec=900)
                    return
                await self._trade(self.open_clip(d, size, s))
                return

    def fit_size(self, d: int, need: float, room: float, mid: float):
        """Самый большой объём <= room, на котором исполнимый спред ещё >= need.
        Тонкий стакан не блокирует вход целиком — берём то, что даёт рынок, но не меньше min_clip_usd."""
        key = "a_open" if d == 1 else "b_open"
        lo = max(self.hl.min_size, self.lt.min_size, self.step(),
                 self.cfg.min_clip_usd / mid, max(self.hl.min_notional, self.lt.min_notional) * 1.05 / mid)
        lo = self.floor_common(lo) if self.floor_common(lo) >= lo else self.floor_common(lo + self.step())
        if room < lo:
            return 0.0, None

        def spread_at(sz):
            return self.exec_spreads(sz).get(key)
        s = spread_at(room)
        if s is not None and s >= need:
            return room, s
        s = spread_at(lo)
        if s is None or s < need:
            return 0.0, s
        best, best_s, a, b = lo, s, lo, room
        for _ in range(16):                     # VWAP-спред монотонно падает с объёмом -> бинпоиск
            m = self.floor_common((a + b) / 2)
            if m <= a:
                break
            sm = spread_at(m)
            if sm is not None and sm >= need:
                best, best_s, a = m, sm, m
            else:
                b = m
        return best, best_s

    def margin_block(self, notional: float) -> str:
        """Причина не входить (пусто = можно)."""
        for v, snap, book in ((self.hl, self.snap_hl, self.hl_book), (self.lt, self.snap_lt, self.lt_book)):
            need = notional / max(v.leverage, 1) * 1.05
            if snap.free - need < snap.equity * self.cfg.free_margin_reserve_pct / 100:
                log.info("%s: мало свободной маржи (free %.2f, нужно %.2f + резерв)", v.name, snap.free, need)
                return f"на {v.name} мало свободной маржи"
            dist = snap.liq_distance(book.mid() or snap.entry)
            if dist is not None and dist < self.cfg.margin_add_below_pct:
                # нога уже под нагрузкой: не наращиваем позицию в разлёт, пока маржа не восстановлена
                log.info("%s под нагрузкой: до ликвидации %.1f%% < %.0f%% — добор запрещён", v.name, dist,
                         self.cfg.margin_add_below_pct)
                return f"{v.name} близко к ликвидации (добор запрещён до восстановления маржи)"
        return ""

    def margin_ok(self, notional: float) -> bool:
        return not self.margin_block(notional)

    async def _trade(self, coro):
        async with self.lock:
            try:
                await coro
            finally:
                self.last_trade = time.time()
                self._cond_since.clear()

    def _log_spread(self, ex):
        if time.time() - self._last_spread_log < self.cfg.spread_log_sec:
            return
        self._last_spread_log = time.time()
        self.spreads.write(self.hl_book.best_bid(), self.hl_book.best_ask(), self.lt_book.best_bid(),
                           self.lt_book.best_ask(), fmt(self.mid_spread(), 3),
                           fmt(ex.get("a_open"), 3), fmt(ex.get("b_open"), 3),
                           self.snap_hl.size, self.snap_lt.size)

    # ------------------------------------------------------------------ риск
    async def risk_tick(self):
        async with self.lock:
            prev = (self.snap_hl.size, self.snap_lt.size)
            try:
                await self.refresh()
            except Exception as e:  # noqa: BLE001
                log.warning("не смог обновить позиции: %s", e)
                return
            cur = (self.snap_hl.size, self.snap_lt.size)
            if self.start_equity is None:
                self.start_equity = self.snap_hl.equity + self.snap_lt.equity
                self.save_state()
            if prev != cur and time.time() - self.last_trade > self.cfg.positions_poll_sec * 2:
                await self.tg.send(f"👀 <b>Позиция изменилась не ботом</b> (ручная сделка или ликвидация?)\n"
                                   f"было HL {fmt(prev[0])} / Lighter {fmt(prev[1])}\n\n" + self.positions_text())
                self._infer_direction()

            if not self.manual_hold and time.time() - self.last_trade > 3:
                net = self.snap_hl.size + self.snap_lt.size
                if abs(net) >= self.step():
                    await self.repair_legs(prefer_complete=False, context="сверка позиций")

            if time.time() - self.last_risk_action < 20:
                return
            for v, snap, book in ((self.hl, self.snap_hl, self.hl_book), (self.lt, self.snap_lt, self.lt_book)):
                dist = snap.liq_distance(book.mid() or snap.entry)
                if dist is None:
                    continue
                if dist < self.cfg.emergency_close_pct:
                    self.last_risk_action = time.time()
                    await self.tg.send(f"🚨 <b>{v.name}: до ликвидации {dist:.1f}%</b> — закрываю спред целиком")
                    await self.close_spread(1.0, f"{v.name}: до ликвидации {dist:.1f}%",
                                            self.cfg.repair_slippage_pct)
                    self.paused = True
                    self.save_state()
                    return
                if dist < self.cfg.margin_add_below_pct:
                    self.last_risk_action = time.time()
                    added = 0.0
                    if snap.isolated:
                        want = snap.notional * (self.cfg.margin_target_pct - dist) / 100
                        avail = max(snap.free - 1.0, 0.0)
                        amt = round(min(want, avail), 2)
                        if amt >= 1 and await v.add_margin(amt):
                            added = amt
                    if added:
                        await self.tg.send(f"💉 {v.name}: до ликвидации было {dist:.1f}% — довнёс маржу ${added:.2f}"
                                           f"{' (хотел больше, свободно мало)' if added < want - 0.5 else ''}\n\n"
                                           + self.positions_text())
                    elif dist < self.cfg.reduce_below_pct:
                        await self.tg.send(f"⚠️ {v.name}: до ликвидации {dist:.1f}%, довнести нечем — "
                                           f"сокращаю спред на {self.cfg.reduce_fraction * 100:.0f}%")
                        await self.close_spread(self.cfg.reduce_fraction, f"{v.name}: до ликвидации {dist:.1f}%",
                                                self.cfg.repair_slippage_pct)
                    else:
                        await self.tg.send(f"⚠️ {v.name}: до ликвидации {dist:.1f}%, "
                                           f"{'cross-маржа' if not snap.isolated else 'нет свободной маржи'}. "
                                           f"Пополни баланс на бирже.", dedupe_sec=600)
                    return

    def _infer_direction(self):
        h, l = self.snap_hl.size, self.snap_lt.size
        if h < 0 < l:
            self.direction = 1
        elif l < 0 < h:
            self.direction = -1
        elif not h and not l:
            self.direction, self.clips, self.exit_stage, self.opened_at = 0, 0, 0, 0.0
        else:
            self.direction = 1 if (h < 0 or l > 0) else (-1 if (h > 0 or l < 0) else 0)
        if self.direction and not self.clips:
            self.clips = 1
        self.save_state()

    # ------------------------------------------------------------------ Telegram
    def positions_text(self) -> str:
        rows = []
        for v, s, b in ((self.hl, self.snap_hl, self.hl_book), (self.lt, self.snap_lt, self.lt_book)):
            mark = b.mid()
            dist = s.liq_distance(mark or s.entry)
            lev = (s.notional / s.margin) if s.margin else None
            rows.append(f"<b>{v.name}</b>: {fmt(s.size, 4)} @ {fmt(s.entry, 5)} | uPnL {fmt(s.upnl, 2)}$\n"
                        f"   ликв. {fmt(s.liq_px, 5)} ({fmt(dist, 1)}%) | маржа {fmt(s.margin, 2)}$ "
                        f"| плечо {fmt(lev, 2)}x | свободно {fmt(s.free, 2)}$")
        eq = self.snap_hl.equity + self.snap_lt.equity
        pnl = eq - self.start_equity if self.start_equity else None
        return "\n".join(rows) + f"\nEquity: {fmt(eq, 2)}$ (с запуска {fmt(pnl, 2)}$)"

    def status_text(self) -> str:
        hb, ha, lb, la = (self.hl_book.best_bid(), self.hl_book.best_ask(),
                          self.lt_book.best_bid(), self.lt_book.best_ask())
        state = "⏸ пауза" if self.paused else ("✋ ручной режим" if self.manual_hold else "▶ работает")
        return (f"📊 <b>XTEN {self.cfg.mode.upper()}</b> {state}\n"
                f"HL {fmt(hb, 5)} / {fmt(ha, 5)} | Lighter {fmt(lb, 5)} / {fmt(la, 5)}\n"
                f"Спред (mid): <b>{fmt(self.mid_spread(), 2)}%</b>  вход ≥ {self.cfg.entry_spread_pct}%, "
                f"выход {', '.join(f'{t}%' for t, _ in self.cfg.exit_levels)}\n"
                f"Направление: {DIR_NAME[self.direction]}, клипов {self.clips}\n\n" + self.positions_text())

    async def on_command(self, action: str) -> str:
        if action == "status":
            try:
                async with self.lock:
                    await self.refresh()
            except Exception as e:  # noqa: BLE001
                log.warning("status refresh: %s", e)
            return self.status_text()
        if action == "pause":
            self.paused = True
            self.save_state()
            return "⏸ Новые входы на паузе. Выходы по схождению и защита от ликвидации работают."
        if action == "resume":
            self.paused = False
            self.manual_hold = False
            async with self.lock:
                await self.refresh()
            self._infer_direction()
            self.save_state()
            return "▶ Работаю. " + self.status_text()
        if action in ("close_hl", "close_lt"):
            v, b = (self.hl, self.hl_book) if action == "close_hl" else (self.lt, self.lt_book)
            async with self.lock:
                self.manual_hold = True
                self.save_state()
                msg = await self.close_leg(v, b)
                self.last_trade = time.time()
            return (f"✖ {html.escape(msg)}\n\n✋ Включён ручной режим: бот не открывает, не выравнивает и не "
                    f"закрывает позиции сам (кроме защиты от ликвидации). Вернуть — «Продолжить».\n\n"
                    + self.positions_text())
        if action == "close_all":
            await self._trade(self.close_spread(1.0, "ручное закрытие из Telegram", self.cfg.repair_slippage_pct))
            self.paused = True
            self.save_state()
            return "⛔ Готово. Новые входы на паузе — «Продолжить», чтобы бот снова торговал."
        return "неизвестная команда"

    # ------------------------------------------------------------------ жизненный цикл
    async def setup_live(self):
        for v, lev, iso in ((self.hl, self.cfg.hl_leverage, self.cfg.hl_isolated),
                            (self.lt, self.cfg.lighter_leverage, self.cfg.lighter_isolated)):
            await v.connect()
            if lev > v.max_leverage:
                log.warning("%s: плечо %sx больше максимума %sx — ставлю %sx", v.name, lev, v.max_leverage, v.max_leverage)
            try:
                await v.set_leverage(lev, iso)
            except Exception as e:  # noqa: BLE001
                # при открытой позиции биржа может не дать сменить режим маржи — работаем с текущим
                log.warning("%s: не смог выставить плечо: %s", v.name, e)
                v.leverage = min(lev, v.max_leverage)
                v.isolated = iso or v.isolated_only

    async def run(self):
        if STOP_FILE.exists():
            STOP_FILE.unlink()
        self.load_state()
        await self.refresh()
        self._infer_direction()
        await self.tg.send(f"🤖 <b>Бот XTEN запущен</b> ({self.cfg.mode})\n"
                           f"HL плечо {self.hl.leverage}x {'iso' if self.hl.isolated else 'cross'}, "
                           f"Lighter {self.lt.leverage}x {'iso' if self.lt.isolated else 'cross'}\n"
                           f"клип ${self.cfg.clip_usd:.0f}, максимум ${self.cfg.max_position_usd:.0f} на ногу\n\n"
                           + self.status_text())
        last_risk = 0.0
        while self.running:
            if STOP_FILE.exists():
                log.info("найден STOP — останавливаюсь (позиции не трогаю)")
                break
            try:
                if time.time() - last_risk >= self.cfg.positions_poll_sec:
                    last_risk = time.time()
                    await self.risk_tick()
                if not self.lock.locked():
                    await self.strategy_tick()
                if self.cfg.status_every_min and time.time() - self._last_status > self.cfg.status_every_min * 60:
                    self._last_status = time.time()
                    await self.tg.send(self.status_text())
            except Exception as e:  # noqa: BLE001
                log.exception("ошибка цикла: %s", e)
                await self.tg.send(f"❗ Ошибка: {html.escape(str(e))[:500]}", buttons=False, dedupe_sec=300)
                await asyncio.sleep(3)
            await asyncio.sleep(self.cfg.loop_sec)
        await self.tg.send("🛑 Бот остановлен. Открытые позиции остались на биржах без присмотра!\n\n"
                           + self.positions_text(), buttons=False)


def setup_logging():
    LOGS.mkdir(exist_ok=True)
    fmt_ = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt_)
    fh = logging.FileHandler(LOGS / "bot.log", encoding="utf-8")
    fh.setFormatter(fmt_)
    root.addHandler(sh)
    root.addHandler(fh)
    for noisy in ("websockets", "urllib3", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


async def main(mode: str | None):
    cfg = config_mod.load(mode_override=mode)
    log.info("режим %s, конфиг: %s", cfg.mode, json.dumps(cfg.public_dict(), ensure_ascii=False))

    hl_real = HyperliquidVenue(cfg.hl_api_url, cfg.hl_coin, cfg.hl_account_address, cfg.hl_agent_private_key)
    lt_real = LighterVenue(cfg.lighter_api_url, cfg.lighter_symbol, cfg.lighter_account_index,
                           cfg.lighter_api_key_index, cfg.lighter_api_private_key)
    hl_book, lt_book = Book("HL"), Book("Lighter")
    if cfg.mode == "paper":
        hl = PaperVenue(hl_real, hl_book, cfg.paper_equity_usd, fee_pct=0.045)
        lt = PaperVenue(lt_real, lt_book, cfg.paper_equity_usd, fee_pct=0.0)
    else:
        hl, lt = hl_real, lt_real
    await asyncio.gather(hl.load_market(), lt.load_market())

    feeds = [HyperliquidFeed(hl_book, cfg.hl_ws_url, cfg.hl_coin),
             LighterFeed(lt_book, cfg.lighter_ws_url, lt_real.market_id)]
    for f in feeds:
        f.start()

    tg = Telegram(cfg.tg_bot_token, cfg.tg_chat_ids, None)
    if not tg.enabled:
        log.warning("Telegram не настроен (TG_BOT_TOKEN / TG_CHAT_ID) — алерты только в лог")
    arb = Arb(cfg, hl, lt, hl_book, lt_book, tg)

    if cfg.mode == "live":
        await arb.setup_live()
    else:
        await hl.set_leverage(cfg.hl_leverage, cfg.hl_isolated)
        await lt.set_leverage(cfg.lighter_leverage, cfg.lighter_isolated)

    for _ in range(40):                       # ждём первые стаканы
        if arb.books_ok():
            break
        await asyncio.sleep(0.5)
    if not arb.books_ok():
        log.warning("стаканы ещё не пришли — бот будет ждать их в цикле")

    tg.start()

    def _stop(*_):
        arb.running = False
    for sig in ("SIGINT", "SIGTERM", "SIGBREAK"):
        if hasattr(signal, sig):
            try:
                signal.signal(getattr(signal, sig), _stop)
            except (ValueError, OSError):
                pass
    try:
        await arb.run()
    finally:
        for f in feeds:
            await f.stop()
        await tg.stop()
        await hl.close()
        await lt.close()
        if isinstance(hl, PaperVenue):
            await hl_real.close()
            await lt_real.close()


if __name__ == "__main__":
    setup_logging()
    m = next((a for a in sys.argv[1:] if a in ("paper", "live")), None)
    try:
        asyncio.run(main(m))
    except KeyboardInterrupt:
        pass
