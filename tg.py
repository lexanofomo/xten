"""
Telegram: алерты + кнопки управления (long polling, без вебхуков и лишних зависимостей).

Кнопки под каждым алертом:
  [Закрыть HL] [Закрыть Lighter]
  [Закрыть спред целиком]
  [Статус] [Пауза / Продолжить]
Закрытие требует подтверждения вторым нажатием. Команды принимаются только из TG_CHAT_ID.
"""
from __future__ import annotations

import asyncio
import html
import logging
import time

import os
import ssl

import aiohttp

log = logging.getLogger("tg")


def _ssl_context() -> ssl.SSLContext:
    """Проверка сертификата по хранилищу Windows (как в Chrome), если стоит truststore.
    Проверку НЕ отключаем: иначе перехватчик увидит токен бота и сможет жать кнопки."""
    try:
        import truststore
        return truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    except Exception:  # noqa: BLE001
        return ssl.create_default_context()

ACTIONS = {
    "close_hl": "закрыть позицию на Hyperliquid",
    "close_lt": "закрыть позицию на Lighter",
    "close_all": "закрыть спред на ОБЕИХ биржах",
}


class Telegram:
    def __init__(self, token: str, chat_ids: list[int], handler):
        """handler: async (action: str) -> str  — выполняет действие, возвращает текст ответа."""
        self.token, self.chat_ids, self.handler = token, chat_ids, handler
        self.enabled = bool(token and chat_ids)
        self.base = f"https://api.telegram.org/bot{token}"
        self.session: aiohttp.ClientSession | None = None
        self._offset = 0
        self._recent: dict[str, float] = {}
        self.paused_getter = lambda: False
        self._task = None
        # TG_PROXY=http://127.0.0.1:PORT — пустить Telegram через локальный прокси VPN-клиента
        self.proxy = os.environ.get("TG_PROXY", "").strip() or None

    def keyboard(self):
        paused = self.paused_getter()
        return {"inline_keyboard": [
            [{"text": "✖ Закрыть HL", "callback_data": "ask:close_hl"},
             {"text": "✖ Закрыть Lighter", "callback_data": "ask:close_lt"}],
            [{"text": "⛔ Закрыть спред целиком", "callback_data": "ask:close_all"}],
            [{"text": "📊 Статус", "callback_data": "do:status"},
             {"text": "▶ Продолжить" if paused else "⏸ Пауза",
              "callback_data": "do:resume" if paused else "do:pause"}],
        ]}

    async def _call(self, method: str, **payload):
        if not self.enabled:
            return None
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=40),
                                                 connector=aiohttp.TCPConnector(ssl=_ssl_context()))
        try:
            async with self.session.post(f"{self.base}/{method}", json=payload, proxy=self.proxy) as r:
                data = await r.json()
                if not data.get("ok"):
                    log.warning("telegram %s: %s", method, data)
                return data
        except Exception as e:  # noqa: BLE001
            log.warning("telegram %s: %s", method, e)
            return None

    async def send(self, text: str, buttons: bool = True, dedupe_sec: float = 0):
        """text — уже экранированный HTML. dedupe_sec — не повторять тот же текст чаще."""
        log.info("TG> %s", text.replace("\n", " | "))
        if dedupe_sec:
            t = self._recent.get(text, 0)
            if time.time() - t < dedupe_sec:
                return
            self._recent[text] = time.time()
        for cid in self.chat_ids:
            payload = dict(chat_id=cid, text=text, parse_mode="HTML", disable_web_page_preview=True)
            if buttons:
                payload["reply_markup"] = self.keyboard()
            await self._call("sendMessage", **payload)

    def start(self):
        if self.enabled:
            self._task = asyncio.create_task(self._poll(), name="telegram")
        return self._task

    async def stop(self):
        if self._task:
            self._task.cancel()
        if self.session and not self.session.closed:
            await self.session.close()

    async def _poll(self):
        # пропускаем то, что накопилось до запуска — старые нажатия не должны ничего закрывать
        d = await self._call("getUpdates", offset=-1, timeout=0)
        if d and d.get("result"):
            self._offset = d["result"][-1]["update_id"] + 1
        while True:
            d = await self._call("getUpdates", offset=self._offset, timeout=25,
                                 allowed_updates=["message", "callback_query"])
            if not d or not d.get("ok"):
                await asyncio.sleep(3)
                continue
            for u in d["result"]:
                self._offset = u["update_id"] + 1
                try:
                    await self.on_update(u)
                except Exception as e:  # noqa: BLE001
                    log.exception("tg update: %s", e)

    async def on_update(self, u: dict):
        if "callback_query" in u:
            q = u["callback_query"]
            chat = q.get("message", {}).get("chat", {}).get("id")
            if chat not in self.chat_ids:
                await self._call("answerCallbackQuery", callback_query_id=q["id"], text="нет доступа")
                return
            await self.on_button(q["data"], chat, q["message"]["message_id"], q["id"])
        elif "message" in u:
            m = u["message"]
            chat = m.get("chat", {}).get("id")
            if chat not in self.chat_ids:
                return
            cmd = (m.get("text") or "").strip().split("@")[0].lower()
            mapping = {"/status": "status", "/pause": "pause", "/resume": "resume",
                       "/start": "status", "/menu": "status"}
            if cmd in mapping:
                await self.send(await self.handler(mapping[cmd]))
            elif cmd in ("/close_hl", "/close_lt", "/close_all"):
                await self._confirm(cmd[1:], chat, None)

    async def _confirm(self, action: str, chat: int, message_id: int | None):
        kb = {"inline_keyboard": [[{"text": "✅ Да, " + ACTIONS[action], "callback_data": f"yes:{action}:{int(time.time())}"}],
                                  [{"text": "Отмена", "callback_data": "no"}]]}
        await self._call("sendMessage", chat_id=chat, parse_mode="HTML", reply_markup=kb,
                         text=f"Подтверди: <b>{html.escape(ACTIONS[action])}</b>?\n"
                              f"Кнопка действует 60 секунд.")

    async def on_button(self, data: str, chat: int, message_id: int, qid: str):
        await self._call("answerCallbackQuery", callback_query_id=qid)
        kind, _, rest = data.partition(":")
        if kind == "ask" and rest in ACTIONS:
            await self._confirm(rest, chat, message_id)
        elif kind == "yes":
            action, _, ts = rest.partition(":")
            if action not in ACTIONS:
                return
            if time.time() - int(ts or 0) > 60:
                await self._call("editMessageText", chat_id=chat, message_id=message_id,
                                 text="Подтверждение устарело, нажми кнопку ещё раз.")
                return
            await self._call("editMessageText", chat_id=chat, message_id=message_id,
                             text=f"Выполняю: {ACTIONS[action]}…")
            await self.send(await self.handler(action))
        elif kind == "no":
            await self._call("editMessageText", chat_id=chat, message_id=message_id, text="Отменено.")
        elif kind == "do" and rest in ("status", "pause", "resume"):
            await self.send(await self.handler(rest))
