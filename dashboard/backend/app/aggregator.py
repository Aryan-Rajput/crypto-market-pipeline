from ast import Call
from dataclasses import dataclass

MINUTE_MS = 60_000


@dataclass
class Bar:
    symbol: str
    minute_ms: int
    open: float
    high: float
    low: float
    close: float
    price_x_qty: float = 0.0
    qty: float = 0.0
    buy_qty: float = 0.0
    sell_qty: float = 0.0
    trades: int = 0

    @classmethod
    def start(cls, symbol, minute_ms, price):
        return cls(symbol, minute_ms, open=price, high=price, low=price, close=price)

    def add(self, price, qty, buyer_is_maker):
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.price_x_qty += price * qty
        self.qty += qty
        if buyer_is_maker:
            self.sell_qty += qty
        else:
            self.buy_qty += qty
        self.trades += 1

    @property
    def vwap(self):
        return self.price_x_qty / self.qty if self.qty else None

    @property
    def ofi(self):
        return (self.buy_qty - self.sell_qty) / self.qty if self.qty else None

    @property
    def volatility(self):
        return (self.high - self.low) / self.open if self.open else None


class Aggregator:
    def __init__(self, drop_startup_bar=True):
        self.drop_startup_bar = drop_startup_bar
        self.current = {}  # symbol -> bar being built
        self.is_startup = {}  # symbol -> true while the current bar is the partial first one
        self.late_dropped = 0

    def mark_gap(self):
        # Call when the feed drops: the bars in progress may be missing trades so drop em
        for symbol in self.current:
            self.is_startup[symbol] = True

    def on_trade(self, symbol, price, qty, buyer_is_maker, trade_ms):
        """Add one trade. Returns the finished bar if this trade closed one, else None."""
        minute = trade_ms // MINUTE_MS * MINUTE_MS
        bar = self.current.get(symbol)
        finished = None

        if bar is None:
            self.is_startup[symbol] = self.drop_startup_bar
        elif minute < bar.minute_ms:
            self.late_dropped += 1
            return None
        elif minute > bar.minute_ms:
            if not self.is_startup[symbol]:
                finished = bar
            self.is_startup[symbol] = False
            bar = None

        if bar is None:
            bar = Bar.start(symbol, minute, price)
            self.current[symbol] = bar
        bar.add(price, qty, buyer_is_maker)
        return finished
