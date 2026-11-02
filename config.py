"""
Конфиг: config.json (параметры стратегии) + .env / переменные окружения (секреты).

Секреты НИКОГДА не кладутся в config.json:
  HL_ACCOUNT_ADDRESS        основной адрес Hyperliquid (где лежат деньги)
  HL_AGENT_PRIVATE_KEY      ключ API-кошелька (agent) Hyperliquid — торговать может, выводить нет
  LIGHTER_ACCOUNT_INDEX     индекс аккаунта / субаккаунта Lighter
  LIGHTER_API_KEY_INDEX     индекс API-ключа Lighter (2..254)
  LIGHTER_API_PRIVATE_KEY   приватный ключ API-ключа Lighter (не кошелька!)
  TG_BOT_TOKEN              токен бота от @BotFather
  TG_CHAT_ID                id чата (или нескольких через запятую), куда слать алерты
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, asdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent


@dataclass
class Config:
    # --- режим ---
    mode: str = "paper"                 # paper | live
    # --- рынок ---
    hl_coin: str = "XTEN"               # имя на Hyperliquid (для HIP-3 было бы "dex:COIN")
    lighter_symbol: str = "XTEN"
    hl_api_url: str = "https://api.hyperliquid.xyz"
    lighter_api_url: str = "https://mainnet.zklighter.elliot.ai"

    # --- стратегия (в процентах) ---
    entry_spread_pct: float = 3.0       # вход, когда исполнимый спред >= 3%
    add_step_pct: float = 1.0           # каждый следующий клип — ещё +1% к спреду
    # выход ступенями: [порог %, доля позиции]. По умолчанию половина при 1%, остаток при 0.5%
    exit_levels: list = field(default_factory=lambda: [[1.0, 0.5], [0.5, 1.0]])
    exit_price: str = "mid"             # mid — схождение средних цен; exec — по ценам, по которым реально закроемся
    confirm_sec: float = 1.0            # условие входа/выхода должно держаться столько секунд
    stop_spread_pct: float = 0.0        # 0 = выкл. Если спред против нас разошёлся до X% — закрыть всё
    max_hold_hours: float = 0.0         # 0 = выкл. Закрыть по времени, даже без схождения

    # --- размеры (USD на ногу) ---
    clip_usd: float = 100.0             # размер одного входа
    min_clip_usd: float = 20.0          # если стакан тонкий — вход меньшим объёмом, но не меньше этого
    max_position_usd: float = 500.0     # потолок на ногу
    max_slippage_pct: float = 0.3       # лимит IOC относительно худшего нужного уровня стакана
    repair_slippage_pct: float = 1.5    # насколько агрессивно добивать отставшую ногу

    # --- плечо и маржа ---
    hl_leverage: int = 2
    lighter_leverage: int = 2
    hl_isolated: bool = True            # для XTEN HL сам может требовать isolated — бот проверит
    lighter_isolated: bool = True
    margin_add_below_pct: float = 25.0  # до ликвидации < 25% -> довнести маржу
    margin_target_pct: float = 40.0     # ...до 40%
    reduce_below_pct: float = 15.0      # < 15% (или довнести нечем) -> сократить спред на reduce_fraction
    reduce_fraction: float = 0.5
    emergency_close_pct: float = 8.0    # < 8% -> закрыть спред целиком
    free_margin_reserve_pct: float = 40.0  # не открываться, если после входа свободно < 40% equity на бирже

    paper_equity_usd: float = 1000.0    # виртуальный баланс на каждой бирже в режиме paper

    # --- служебное ---
    loop_sec: float = 0.5               # шаг основного цикла
    positions_poll_sec: float = 5.0     # сверка позиций и риска через REST
    book_stale_sec: float = 10.0        # стакан старше — не торгуем
    min_trade_interval_sec: float = 3.0
    status_every_min: float = 60.0      # периодический статус в Telegram, 0 = выкл
    spread_log_sec: float = 5.0

    # секреты (заполняются из env, в json не пишутся)
    hl_account_address: str = ""
    hl_agent_private_key: str = ""
    lighter_account_index: int = -1
    lighter_api_key_index: int = -1
    lighter_api_private_key: str = ""
    tg_bot_token: str = ""
    tg_chat_ids: list = field(default_factory=list)

    SECRET_FIELDS = ("hl_account_address", "hl_agent_private_key", "lighter_account_index",
                     "lighter_api_key_index", "lighter_api_private_key", "tg_bot_token", "tg_chat_ids")

    @property
    def lighter_ws_url(self) -> str:
        return self.lighter_api_url.replace("https://", "wss://").rstrip("/") + "/stream"

    @property
    def hl_ws_url(self) -> str:
        return self.hl_api_url.replace("https://", "wss://").rstrip("/") + "/ws"

    def public_dict(self) -> dict:
        d = asdict(self)
        for k in self.SECRET_FIELDS:
            d.pop(k, None)
        return d

    def validate(self):
        errs = []
        if self.exit_price not in ("mid", "exec"):
            errs.append("exit_price должен быть mid или exec")
        if self.mode not in ("paper", "live"):
            errs.append("mode должен быть paper или live")
        lv = sorted(self.exit_levels, key=lambda x: -x[0])
        if not lv or lv[-1][1] < 1.0:
            errs.append("последняя ступень exit_levels должна закрывать долю 1.0")
        if any(x[0] >= self.entry_spread_pct for x in lv):
            errs.append("пороги выхода должны быть меньше entry_spread_pct")
        if not (self.emergency_close_pct < self.reduce_below_pct < self.margin_add_below_pct < self.margin_target_pct):
            errs.append("нужно emergency_close_pct < reduce_below_pct < margin_add_below_pct < margin_target_pct")
        if self.clip_usd < 11:
            errs.append("clip_usd < $11: Hyperliquid не примет ордер меньше $10")
        if self.mode == "live":
            for k in ("hl_account_address", "hl_agent_private_key", "lighter_api_private_key"):
                if not getattr(self, k):
                    errs.append(f"для live нужен {k.upper()} в .env")
            if self.lighter_account_index < 0 or self.lighter_api_key_index < 0:
                errs.append("для live нужны LIGHTER_ACCOUNT_INDEX и LIGHTER_API_KEY_INDEX")
        if errs:
            raise SystemExit("Ошибки конфига:\n  - " + "\n  - ".join(errs))


def _load_dotenv(path: Path):
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def load(path: str | Path = ROOT / "config.json", mode_override: str | None = None) -> Config:
    _load_dotenv(ROOT / ".env")
    path = Path(path)
    cfg = Config()
    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
        for k, v in data.items():
            if k.startswith("_"):
                continue
            if k in Config.SECRET_FIELDS:
                continue
            if not hasattr(cfg, k):
                raise SystemExit(f"config.json: неизвестный параметр {k!r}")
            setattr(cfg, k, v)
    else:
        path.write_text(json.dumps(cfg.public_dict(), indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"создан {path} с настройками по умолчанию")

    e = os.environ
    cfg.hl_account_address = e.get("HL_ACCOUNT_ADDRESS", "").strip()
    cfg.hl_agent_private_key = e.get("HL_AGENT_PRIVATE_KEY", "").strip()
    cfg.lighter_account_index = int(e.get("LIGHTER_ACCOUNT_INDEX", "-1") or -1)
    cfg.lighter_api_key_index = int(e.get("LIGHTER_API_KEY_INDEX", "-1") or -1)
    cfg.lighter_api_private_key = e.get("LIGHTER_API_PRIVATE_KEY", "").strip()
    cfg.tg_bot_token = e.get("TG_BOT_TOKEN", "").strip()
    cfg.tg_chat_ids = [int(x) for x in e.get("TG_CHAT_ID", "").replace(" ", "").split(",") if x]
    if mode_override:
        cfg.mode = mode_override
    cfg.exit_levels = sorted([[float(a), float(b)] for a, b in cfg.exit_levels], key=lambda x: -x[0])
    cfg.validate()
    return cfg
