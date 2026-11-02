"""
Проверка рынка без ключей: параметры XTEN на обеих биржах, живые стаканы, спред, глубина.
    python check.py            (20 секунд наблюдения)
    python check.py 120        (2 минуты)
"""
import asyncio
import sys
import time

import config as config_mod
from books import Book, HyperliquidFeed, LighterFeed
from venues import HyperliquidVenue, LighterVenue


async def main(seconds: int):
    cfg = config_mod.load(mode_override="paper")
    hl = HyperliquidVenue(cfg.hl_api_url, cfg.hl_coin)
    lt = LighterVenue(cfg.lighter_api_url, cfg.lighter_symbol)
    await hl.load_market()
    await lt.load_market()
    print(f"\nHyperliquid {cfg.hl_coin}: шаг {hl.size_step}, макс. плечо {hl.max_leverage}x, "
          f"только isolated: {hl.isolated_only}, mark {hl.ctx.get('markPx')}, funding/час {hl.ctx.get('funding')}")
    print(f"Lighter {cfg.lighter_symbol}: market_id {lt.market_id}, шаг {lt.size_step}, мин. размер {lt.min_size}, "
          f"мин. ордер ${lt.min_quote}, макс. плечо {lt.max_leverage}x, taker fee {lt.taker_fee}")
    if cfg.hl_leverage > hl.max_leverage or cfg.lighter_leverage > lt.max_leverage:
        print("!! плечо в config.json выше максимума биржи — бот срежет до максимума")

    hb, lb = Book("HL"), Book("Lighter")
    feeds = [HyperliquidFeed(hb, cfg.hl_ws_url, cfg.hl_coin), LighterFeed(lb, cfg.lighter_ws_url, lt.market_id)]
    for f in feeds:
        f.start()
    t_end = time.time() + seconds
    mx = -1e9
    mn = 1e9
    while time.time() < t_end:
        await asyncio.sleep(2)
        if not (hb.fresh(10) and lb.fresh(10)):
            print("жду стаканы…")
            continue
        s = (hb.mid() / lb.mid() - 1) * 100
        mx, mn = max(mx, s), min(mn, s)
        print(f"HL {hb.best_bid():.5g}/{hb.best_ask():.5g}  Lighter {lb.best_bid():.5g}/{lb.best_ask():.5g}  "
              f"спред {s:+.2f}%  | глубина ±1%: HL ${hb.depth_usd(True, 1):.0f}/${hb.depth_usd(False, 1):.0f}  "
              f"Lighter ${lb.depth_usd(True, 1):.0f}/${lb.depth_usd(False, 1):.0f}")
    for f in feeds:
        await f.stop()
    await hl.close()
    await lt.close()
    if mx > -1e9:
        print(f"\nспред за период: от {mn:+.2f}% до {mx:+.2f}%  (вход при |спред| ≥ {cfg.entry_spread_pct}%)")


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1]) if len(sys.argv) > 1 else 20))
