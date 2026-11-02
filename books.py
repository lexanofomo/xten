"""
Стаканы Hyperliquid и Lighter по WebSocket.

Каждый фид держит локальную книгу, время последнего обновления и сам переподключается.
Торговая логика читает только Book.ts / Book.fresh() и Book.sweep().
"""
from __future__ import annotations

import asyncio
import json
import logging
import time

import websockets

log = logging.getLogger("books")


class Book:
    def __init__(self, name: str):
        self.name = name
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.ts = 0.0

    # ---- чтение ----
    def best_bid(self):
        return max(self.bids) if self.bids else None

    def best_ask(self):
        return min(self.asks) if self.asks else None

    def mid(self):
        b, a = self.best_bid(), self.best_ask()
        return (a + b) / 2 if a and b else None

    def fresh(self, stale_sec: float) -> bool:
        b, a = self.best_bid(), self.best_ask()
        return bool(b and a and b < a and time.time() - self.ts < stale_sec)

    def sweep(self, is_buy: bool, size: float):
        """Сколько стоит забрать size с рынка: (vwap, худшая цена, доступный объём).
        Если глубины не хватает — vwap по тому, что есть, и available < size."""
        levels = sorted(self.asks.items()) if is_buy else sorted(self.bids.items(), reverse=True)
        left, cost, worst = size, 0.0, None
        for px, sz in levels:
            take = min(left, sz)
            cost += take * px
            left -= take
            worst = px
            if left <= 1e-12:
                break
        got = size - max(left, 0.0)
        return (cost / got if got > 0 else None), worst, got

    def depth_usd(self, is_buy: bool, within_pct: float) -> float:
        ref = self.best_ask() if is_buy else self.best_bid()
        if not ref:
            return 0.0
        if is_buy:
            return sum(p * s for p, s in self.asks.items() if p <= ref * (1 + within_pct / 100))
        return sum(p * s for p, s in self.bids.items() if p >= ref * (1 - within_pct / 100))

    # ---- запись ----
    def set_snapshot(self, bids, asks):
        self.bids = {p: s for p, s in bids if s > 0}
        self.asks = {p: s for p, s in asks if s > 0}
        self.ts = time.time()

    def apply_delta(self, bids, asks):
        for side, upd in ((self.bids, bids), (self.asks, asks)):
            for p, s in upd:
                if s <= 0:
                    side.pop(p, None)
                else:
                    side[p] = s
        self.ts = time.time()


class _Feed:
    def __init__(self, book: Book, url: str):
        self.book, self.url = book, url
        self._task = None
        self.connected = False
        self.reconnects = 0

    def start(self):
        self._task = asyncio.create_task(self._run(), name=f"feed-{self.book.name}")
        return self._task

    async def stop(self):
        if self._task:
            self._task.cancel()

    async def _run(self):
        backoff = 1
        while True:
            try:
                async with websockets.connect(self.url, ping_interval=20, ping_timeout=20,
                                              max_size=2 ** 23, open_timeout=15) as ws:
                    self.connected = True
                    backoff = 1
                    await self._subscribe(ws)
                    log.info("%s: WS подключён", self.book.name)
                    await self._loop(ws)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning("%s: WS обрыв: %s", self.book.name, e)
            self.connected = False
            self.book.ts = 0.0          # сразу считаем стакан протухшим
            self.reconnects += 1
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


class HyperliquidFeed(_Feed):
    """l2Book: каждое сообщение — полный снимок (до 20 уровней на сторону)."""

    def __init__(self, book: Book, ws_url: str, coin: str):
        super().__init__(book, ws_url)
        self.coin = coin

    async def _subscribe(self, ws):
        await ws.send(json.dumps({"method": "subscribe",
                                  "subscription": {"type": "l2Book", "coin": self.coin}}))

    async def _loop(self, ws):
        last_ping = time.time()
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=30)
            except asyncio.TimeoutError:
                raw = None
            if time.time() - last_ping > 25:
                await ws.send(json.dumps({"method": "ping"}))
                last_ping = time.time()
            if raw is None:
                continue
            msg = json.loads(raw)
            if msg.get("channel") == "l2Book":
                self.handle(msg["data"])

    def handle(self, data):
        lv = data["levels"]
        self.book.set_snapshot([(float(x["px"]), float(x["sz"])) for x in lv[0]],
                               [(float(x["px"]), float(x["sz"])) for x in lv[1]])


class LighterFeed(_Feed):
    """order_book/{market_id}: снимок при подписке, дальше дельты (size 0 = уровень удалён).
    Раз в resync_sec переподписываемся, чтобы гарантированно не копить расхождение."""

    def __init__(self, book: Book, ws_url: str, market_id: int, resync_sec: float = 300):
        super().__init__(book, ws_url)
        self.market_id = market_id
        self.resync_sec = resync_sec

    async def _subscribe(self, ws):
        # чистим книгу: первый ответ после подписки — полный снимок, старые уровни не должны остаться
        self.book.bids, self.book.asks, self.book.ts = {}, {}, 0.0
        self._nonce = None
        await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{self.market_id}"}))
        self._subscribed_at = time.time()

    async def _loop(self, ws):
        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=60)
            msg = json.loads(raw)
            t = msg.get("type", "")
            if t == "ping":
                await ws.send(json.dumps({"type": "pong"}))
                continue
            gap = self.handle(msg) is False
            b, a = self.book.best_bid(), self.book.best_ask()
            crossed = b is not None and a is not None and b >= a
            if gap or crossed or time.time() - self._subscribed_at > self.resync_sec:
                if crossed or gap:
                    log.warning("lighter: %s — переподписка", "пропуск обновлений" if gap else f"перекрещенный стакан {b}/{a}")
                    self.book.ts = 0.0
                await ws.send(json.dumps({"type": "unsubscribe", "channel": f"order_book/{self.market_id}"}))
                await self._subscribe(ws)

    def handle(self, msg):
        t = msg.get("type", "")
        ob = msg.get("order_book")
        if not ob:
            return
        bids = [(float(x["price"]), float(x["size"])) for x in ob.get("bids", [])]
        asks = [(float(x["price"]), float(x["size"])) for x in ob.get("asks", [])]
        nonce, begin = ob.get("nonce"), ob.get("begin_nonce")
        if t.startswith("subscribed"):
            self.book.set_snapshot(bids, asks)
        elif t.startswith("update"):
            last = getattr(self, "_nonce", None)
            if last is not None and begin is not None and begin != last:
                self._nonce = None
                return False                      # пропустили кусок — нужен новый снимок
            self.book.apply_delta(bids, asks)
        self._nonce = nonce
        return True
