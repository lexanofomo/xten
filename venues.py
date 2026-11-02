"""
Биржи под единым интерфейсом.

    await v.load_market()                      метаданные рынка (шаг размера, минимумы, макс. плечо)
    await v.fetch() -> Snapshot                позиция + баланс + ликвидационная цена
    await v.ioc(is_buy, size, limit_px, reduce_only) -> Fill   агрессивная IOC-заявка
    await v.set_leverage(lev, isolated)
    await v.add_margin(usd) -> bool            довнести изолированную маржу

Чтения идут прямыми REST-запросами (aiohttp), подпись — официальными SDK.
PaperVenue исполняет по живому стакану виртуально — для режима paper и тестов.
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_UP, ROUND_HALF_UP

import aiohttp

log = logging.getLogger("venue")


@dataclass
class Snapshot:
    size: float = 0.0          # знаковый размер, + лонг / - шорт
    entry: float = 0.0
    liq_px: float | None = None
    margin: float = 0.0        # маржа под позицией (изолированная) или 0
    notional: float = 0.0
    upnl: float = 0.0
    equity: float = 0.0        # стоимость аккаунта
    free: float = 0.0          # свободная маржа / доступно
    isolated: bool = True
    ts: float = 0.0

    def liq_distance(self, mark: float) -> float | None:
        """Сколько процентов цене идти до ликвидации (None, если позиции нет или биржа не дала цену)."""
        if not self.size or not self.liq_px or not mark:
            return None
        return abs(mark - self.liq_px) / mark * 100


@dataclass
class Fill:
    filled: float = 0.0        # сколько исполнилось (всегда >= 0)
    avg_px: float = 0.0
    error: str = ""


def _dec(x) -> Decimal:
    return Decimal(str(x))


class VenueError(Exception):
    pass


class BaseVenue:
    name = "base"
    isolated_only = False

    def __init__(self):
        self.size_step = 0.0
        self.min_size = 0.0
        self.min_notional = 0.0
        self.max_leverage = 1
        self.leverage = 1
        self.isolated = True
        self.session: aiohttp.ClientSession | None = None

    async def _http(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        return self.session

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    def floor_size(self, size: float) -> float:
        if self.size_step <= 0:
            return size
        q = _dec(self.size_step)
        n = _dec(abs(size)) / q
        near = n.to_integral_value(ROUND_HALF_UP)
        if abs(n - near) < Decimal("1e-6"):      # 0.09999999999 при шаге 0.1 — это 0.1, а не 0
            n = near
        return float(n.to_integral_value(ROUND_DOWN) * q)

    def can_trade(self, size: float, px: float, reduce_only: bool = False) -> bool:
        if size <= 0 or size < self.min_size:
            return False
        if reduce_only:          # закрывающие reduce-only ордера на HL разрешены и меньше $10
            return True
        return size * px >= self.min_notional


# ===================================================================== Hyperliquid
class HyperliquidVenue(BaseVenue):
    name = "Hyperliquid"

    def __init__(self, api_url: str, coin: str, account: str = "", agent_key: str = ""):
        super().__init__()
        self.api_url = api_url.rstrip("/")
        self.coin = coin
        self.dex = coin.split(":")[0] if ":" in coin else ""
        self.account = account
        self.agent_key = agent_key
        self.sz_decimals = 0
        self.min_notional = 10.0
        self.exchange = None

    async def _info(self, body: dict):
        s = await self._http()
        async with s.post(self.api_url + "/info", json=body) as r:
            if r.status != 200:
                raise VenueError(f"HL info {body.get('type')}: HTTP {r.status} {await r.text()}")
            return await r.json()

    async def load_market(self):
        body = {"type": "metaAndAssetCtxs"}
        if self.dex:
            body["dex"] = self.dex
        meta, ctxs = await self._info(body)
        for i, a in enumerate(meta["universe"]):
            if a["name"] == self.coin:
                if a.get("isDelisted"):
                    raise VenueError(f"{self.coin} делистнут на Hyperliquid")
                self.sz_decimals = int(a["szDecimals"])
                self.size_step = 10 ** -self.sz_decimals
                self.min_size = self.size_step
                self.max_leverage = int(a.get("maxLeverage", 1))
                self.isolated_only = bool(a.get("onlyIsolated")) or a.get("marginMode") in ("strictIsolated", "noCross")
                self.ctx = ctxs[i]
                log.info("HL %s: szDecimals=%s maxLev=%s onlyIsolated=%s mark=%s funding=%s",
                         self.coin, self.sz_decimals, self.max_leverage, self.isolated_only,
                         self.ctx.get("markPx"), self.ctx.get("funding"))
                return
        names = [a["name"] for a in meta["universe"] if self.coin.split(":")[-1] in a["name"]]
        raise VenueError(f"{self.coin} не найден на Hyperliquid. Похожие: {names}")

    async def connect(self):
        """Создаёт подписывающий Exchange (только для live)."""
        from eth_account import Account
        from hyperliquid.exchange import Exchange
        wallet = Account.from_key(self.agent_key)
        kw = dict(account_address=self.account)
        if self.dex:
            kw["perp_dexs"] = ["", self.dex]
        self.exchange = await asyncio.to_thread(Exchange, wallet, self.api_url, **kw)
        log.info("HL: агент %s торгует за %s", wallet.address, self.account)

    def round_px(self, px: float, is_buy: bool) -> float:
        """5 значащих цифр и не больше (6 - szDecimals) знаков; округляем в «разрешающую» сторону."""
        decimals = max(6 - self.sz_decimals, 0)
        q_dec = Decimal(1).scaleb(-decimals)
        q_sig = Decimal(1).scaleb(math.floor(math.log10(px)) - 4)
        q = max(q_dec, q_sig)
        mode = ROUND_UP if is_buy else ROUND_DOWN
        return float((_dec(px) / q).to_integral_value(mode) * q)

    async def fetch(self) -> Snapshot:
        body = {"type": "clearinghouseState", "user": self.account}
        if self.dex:
            body["dex"] = self.dex
        st = await self._info(body)
        snap = Snapshot(ts=time.time(), isolated=self.isolated)
        ms = st.get("marginSummary", {})
        snap.equity = float(ms.get("accountValue", 0))
        snap.free = float(st.get("withdrawable", 0))
        for ap in st.get("assetPositions", []):
            p = ap["position"]
            if p["coin"] != self.coin:
                continue
            snap.size = float(p["szi"])
            snap.entry = float(p.get("entryPx") or 0)
            snap.liq_px = float(p["liquidationPx"]) if p.get("liquidationPx") else None
            snap.margin = float(p.get("marginUsed", 0))
            snap.notional = float(p.get("positionValue", 0))
            snap.upnl = float(p.get("unrealizedPnl", 0))
            snap.isolated = p.get("leverage", {}).get("type") == "isolated"
        return snap

    @staticmethod
    def _parse(resp) -> Fill:
        # Ошибка может прийти внутри status:ok — смотрим statuses (выучено на NVDA-хедже).
        if not isinstance(resp, dict) or resp.get("status") != "ok":
            return Fill(error=f"HL: {resp}")
        try:
            st = resp["response"]["data"]["statuses"][0]
        except (KeyError, IndexError, TypeError):
            return Fill(error=f"HL: неожиданный ответ {resp}")
        if "error" in st:
            return Fill(error=f"HL: {st['error']}")
        if "filled" in st:
            f = st["filled"]
            return Fill(filled=float(f["totalSz"]), avg_px=float(f["avgPx"]))
        return Fill(error=f"HL: IOC не исполнен ({st})")

    async def ioc(self, is_buy: bool, size: float, limit_px: float, reduce_only: bool = False) -> Fill:
        size = self.floor_size(size)
        px = self.round_px(limit_px, is_buy)
        try:
            resp = await asyncio.to_thread(self.exchange.order, self.coin, is_buy, size, px,
                                           {"limit": {"tif": "Ioc"}}, reduce_only)
        except Exception as e:  # noqa: BLE001
            return Fill(error=f"HL: {e}")
        return self._parse(resp)

    async def set_leverage(self, lev: int, isolated: bool):
        isolated = isolated or self.isolated_only
        lev = max(1, min(int(lev), self.max_leverage))
        resp = await asyncio.to_thread(self.exchange.update_leverage, lev, self.coin, not isolated)
        if not isinstance(resp, dict) or resp.get("status") != "ok":
            raise VenueError(f"HL update_leverage: {resp}")
        self.leverage, self.isolated = lev, isolated
        log.info("HL: плечо %sx, %s", lev, "isolated" if isolated else "cross")

    async def add_margin(self, usd: float) -> bool:
        resp = await asyncio.to_thread(self.exchange.update_isolated_margin, round(usd, 2), self.coin)
        ok = isinstance(resp, dict) and resp.get("status") == "ok"
        if not ok:
            log.warning("HL add_margin %.2f: %s", usd, resp)
        return ok


# ===================================================================== Lighter
class LighterVenue(BaseVenue):
    name = "Lighter"

    def __init__(self, api_url: str, symbol: str, account_index: int = -1,
                 api_key_index: int = -1, api_key: str = ""):
        super().__init__()
        self.api_url = api_url.rstrip("/")
        self.symbol = symbol
        self.account_index = account_index
        self.api_key_index = api_key_index
        self.api_key = api_key
        self.market_id = -1
        self.size_decimals = 0
        self.price_decimals = 0
        self.min_quote = 0.0
        self.signer = None
        self._coi = int(time.time() * 1000) % 10 ** 12

    async def _get(self, path: str, **params):
        s = await self._http()
        async with s.get(self.api_url + path, params=params) as r:
            if r.status != 200:
                raise VenueError(f"Lighter {path}: HTTP {r.status} {await r.text()}")
            return await r.json()

    async def load_market(self):
        d = await self._get("/api/v1/orderBookDetails")
        markets = (d.get("order_book_details") or []) + (d.get("spot_order_book_details") or [])
        for m in markets:
            if str(m.get("symbol", "")).upper() == self.symbol.upper():
                if m.get("status") not in (None, "active"):
                    log.warning("Lighter %s: статус рынка %s", self.symbol, m.get("status"))
                self.market_id = int(m["market_id"])
                self.size_decimals = int(m["size_decimals"])
                self.price_decimals = int(m["price_decimals"])
                self.size_step = 10 ** -self.size_decimals
                self.min_size = max(float(m.get("min_base_amount") or 0), self.size_step)
                self.min_quote = float(m.get("min_quote_amount") or 0)
                self.min_notional = self.min_quote
                imf = float(m.get("min_initial_margin_fraction") or 10000)
                self.max_leverage = max(1, int(10000 / imf))
                self.mmf = float(m.get("maintenance_margin_fraction") or 0) / 10000
                self.taker_fee = m.get("taker_fee")
                log.info("Lighter %s: market_id=%s size_dec=%s price_dec=%s min_base=%s min_quote=%s maxLev=%s taker=%s",
                         self.symbol, self.market_id, self.size_decimals, self.price_decimals,
                         self.min_size, self.min_quote, self.max_leverage, self.taker_fee)
                return
        raise VenueError(f"{self.symbol} не найден на Lighter "
                         f"(есть: {sorted(m.get('symbol') for m in markets)[:60]}…)")

    async def connect(self):
        import lighter
        self.signer = lighter.SignerClient(url=self.api_url, account_index=self.account_index,
                                           api_private_keys={self.api_key_index: self.api_key})
        err = self.signer.check_client()
        if err:
            raise VenueError(f"Lighter: ключ не принят: {err}")
        log.info("Lighter: аккаунт %s, api key %s", self.account_index, self.api_key_index)

    async def close(self):
        if self.signer:
            try:
                await self.signer.close()
            except Exception:  # noqa: BLE001
                pass
        await super().close()

    def _next_coi(self) -> int:
        self._coi += 1
        return self._coi

    def _px_int(self, px: float, is_buy: bool) -> int:
        mode = ROUND_UP if is_buy else ROUND_DOWN
        return int((_dec(px) * (10 ** self.price_decimals)).to_integral_value(mode))

    def _sz_int(self, size: float) -> int:
        return int((_dec(size) * (10 ** self.size_decimals)).to_integral_value(ROUND_HALF_UP))

    async def fetch(self) -> Snapshot:
        d = await self._get("/api/v1/account", by="index", value=str(self.account_index))
        acc = (d.get("accounts") or [{}])[0]
        snap = Snapshot(ts=time.time(), isolated=self.isolated)
        snap.equity = float(acc.get("total_asset_value") or acc.get("collateral") or 0)
        snap.free = float(acc.get("available_balance") or 0)
        for p in acc.get("positions") or []:
            if int(p.get("market_id", -1)) != self.market_id:
                continue
            sz = float(p.get("position") or 0)
            snap.size = sz * (1 if int(p.get("sign", 1)) >= 0 else -1)
            snap.entry = float(p.get("avg_entry_price") or 0)
            lp = float(p.get("liquidation_price") or 0)
            snap.liq_px = lp if lp > 0 and sz else None
            snap.margin = float(p.get("allocated_margin") or 0)
            snap.notional = abs(float(p.get("position_value") or 0))
            snap.upnl = float(p.get("unrealized_pnl") or 0)
            snap.isolated = int(p.get("margin_mode", 1)) == 1
        return snap

    async def _send_check(self, coro, what: str):
        tx, resp, err = await coro
        if err:
            raise VenueError(f"Lighter {what}: {err}")
        if resp is not None and getattr(resp, "code", 200) != 200:
            raise VenueError(f"Lighter {what}: code={resp.code} {resp.message}")
        return resp

    async def ioc(self, is_buy: bool, size: float, limit_px: float, reduce_only: bool = False) -> Fill:
        """send_tx не возвращает исполнение, поэтому меряем его по изменению позиции."""
        size = self.floor_size(size)
        try:
            before = (await self.fetch()).size
            await self._send_check(self.signer.create_order(
                market_index=self.market_id, client_order_index=self._next_coi(),
                base_amount=self._sz_int(size), price=self._px_int(limit_px, is_buy),
                is_ask=not is_buy, order_type=self.signer.ORDER_TYPE_LIMIT,
                time_in_force=self.signer.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                reduce_only=reduce_only, order_expiry=self.signer.DEFAULT_IOC_EXPIRY), "IOC")
        except Exception as e:  # noqa: BLE001
            return Fill(error=str(e))
        filled, deadline = 0.0, time.time() + 4
        while time.time() < deadline:
            await asyncio.sleep(0.4)
            try:
                after = (await self.fetch()).size
            except Exception:  # noqa: BLE001
                continue
            filled = abs(after - before)
            if filled >= size * 0.999:
                break
        if filled <= 0:
            return Fill(error="Lighter: IOC не исполнен (позиция не изменилась)")
        return Fill(filled=round(filled, self.size_decimals), avg_px=limit_px)

    async def set_leverage(self, lev: int, isolated: bool):
        lev = max(1, min(int(lev), self.max_leverage))
        mode = self.signer.ISOLATED_MARGIN_MODE if isolated else self.signer.CROSS_MARGIN_MODE
        await self._send_check(self.signer.update_leverage(self.market_id, mode, lev), "update_leverage")
        self.leverage, self.isolated = lev, isolated
        log.info("Lighter: плечо %sx, %s", lev, "isolated" if isolated else "cross")

    async def add_margin(self, usd: float) -> bool:
        try:
            await self._send_check(self.signer.update_margin(
                self.market_id, round(usd, 2), self.signer.ISOLATED_MARGIN_ADD_COLLATERAL), "update_margin")
            return True
        except Exception as e:  # noqa: BLE001
            log.warning("Lighter add_margin %.2f: %s", usd, e)
            return False


# ===================================================================== Paper
class PaperVenue(BaseVenue):
    """Виртуальное исполнение по живому стакану. Метаданные берёт у настоящей биржи."""

    def __init__(self, real: BaseVenue, book, equity: float = 1000.0, fee_pct: float = 0.045):
        super().__init__()
        self.real, self.book = real, book
        self.name = real.name + "(paper)"
        self.cash = equity
        self.pos = 0.0
        self.entry = 0.0
        self.margin_alloc = 0.0
        self.fee = fee_pct / 100
        self.mmf = 0.5 / max(real.max_leverage, 1)

    async def load_market(self):
        await self.real.load_market()
        for k in ("size_step", "min_size", "min_notional", "max_leverage", "isolated_only"):
            setattr(self, k, getattr(self.real, k))
        self.mmf = 0.5 / max(self.max_leverage, 1)

    async def connect(self):
        pass

    def _mark(self):
        return self.book.mid() or self.entry

    async def fetch(self) -> Snapshot:
        m = self._mark()
        upnl = (m - self.entry) * self.pos if self.pos else 0.0
        s = Snapshot(size=self.pos, entry=self.entry, margin=self.margin_alloc,
                     notional=abs(self.pos) * m, upnl=upnl, ts=time.time(), isolated=self.isolated)
        s.equity = self.cash + upnl
        s.free = self.cash - self.margin_alloc
        if self.pos:
            q = abs(self.pos)
            if self.pos > 0:
                s.liq_px = max((self.entry * q - self.margin_alloc) / (q * (1 - self.mmf)), 0.0)
            else:
                s.liq_px = (self.entry * q + self.margin_alloc) / (q * (1 + self.mmf))
        return s

    async def ioc(self, is_buy: bool, size: float, limit_px: float, reduce_only: bool = False) -> Fill:
        size = self.floor_size(size)
        if reduce_only:
            if self.pos == 0 or (self.pos > 0) == is_buy:
                return Fill(error="reduce-only: нечего сокращать")
            size = min(size, abs(self.pos))
        levels = sorted(self.book.asks.items()) if is_buy else sorted(self.book.bids.items(), reverse=True)
        left, cost = size, 0.0
        for px, sz in levels:
            if (is_buy and px > limit_px) or (not is_buy and px < limit_px):
                break
            take = min(left, sz)
            cost += take * px
            left -= take
            if left <= 1e-12:
                break
        got = self.floor_size(size - left)
        if got <= 0:
            return Fill(error="paper: нет ликвидности в пределах лимита")
        avg = cost / (size - left)
        self._apply(got if is_buy else -got, avg)
        return Fill(filled=got, avg_px=avg)

    def _apply(self, d: float, px: float):
        self.cash -= abs(d) * px * self.fee
        old = self.pos
        if old == 0 or (old > 0) == (d > 0):          # наращиваем
            new = old + d
            self.entry = (self.entry * abs(old) + px * abs(d)) / abs(new)
            self.margin_alloc += abs(d) * px / self.leverage
            self.pos = new
            return
        closed = min(abs(d), abs(old))                  # сокращаем
        self.cash += (px - self.entry) * closed * (1 if old > 0 else -1)
        self.margin_alloc *= (abs(old) - closed) / abs(old)
        new = old + d
        if abs(new) < 1e-12:
            self.pos, self.entry, self.margin_alloc = 0.0, 0.0, 0.0
        elif (new > 0) != (old > 0):                    # перевернулись
            self.pos, self.entry = new, px
            self.margin_alloc = abs(new) * px / self.leverage
        else:
            self.pos = new

    async def set_leverage(self, lev: int, isolated: bool):
        self.leverage = max(1, min(int(lev), self.max_leverage))
        self.isolated = isolated or self.isolated_only

    async def add_margin(self, usd: float) -> bool:
        if usd > self.cash - self.margin_alloc:
            return False
        self.margin_alloc += usd
        return True
