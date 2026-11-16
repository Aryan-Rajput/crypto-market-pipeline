import asyncio
import json
import logging

import websockets

URL = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade"
RECONNECT_DELAY = 3  # dis is in seconds

log = logging.getLogger(__name__)


async def run_feed(aggregator, on_bar, url=URL):
    # read trades forever and pass each finished bar to on_bar (an async function)
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                log.info("feed connected")
                async for raw in ws:
                    trade = json.loads(raw)["data"]
                    bar = aggregator.on_trade(
                        trade["s"],
                        float(trade["p"]),
                        float(trade["q"]),
                        trade["m"],
                        trade["T"],
                    )
                    if bar is not None:
                        await on_bar(bar)
        except (websockets.WebSocketException, OSError) as e:
            log.warning("feed dropped: %s", e)
        aggregator.mark_gap()
        await asyncio.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    from app.aggregator import Aggregator

    async def show(bar):
        print(
            bar.symbol,
            bar.minute_ms,
            f"vwap={bar.vwap:.2f}",
            f"ofi={bar.ofi:+.3f}",
            f"vol={bar.volatility:.5f}",
            f"trades={bar.trades}",
        )

    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_feed(Aggregator(), show))
