"""
SignalSnapshot - restricted market data for signal generation.

This class provides ONLY the data that is available at the time of signal calculation,
preventing lookahead bias in backtesting.

At T-day open:
- Futures: open, pre_settle (known)
- Futures: close, settle, volume, oi (UNKNOWN - cannot access)
- Index: open (known), close (UNKNOWN)
"""
from dataclasses import dataclass
from datetime import date
from typing import Dict, Optional, Literal

from loguru import logger

from ..domain.bars import IndexDailyBar, FuturesDailyBar


@dataclass(frozen=True)
class RestrictedFuturesBar:
    """
    Restricted futures bar - only contains data available at signal time.
    """
    trade_date: date
    ts_code: str
    open: float
    pre_settle: float  # Previous day's settlement price
    
    # T-1 day data (fully known)
    prev_close: Optional[float] = None
    prev_settle: Optional[float] = None
    prev_volume: Optional[float] = None
    prev_oi: Optional[float] = None


@dataclass(frozen=True)
class RestrictedIndexBar:
    """
    Restricted index bar - only contains data available at signal time.
    """
    trade_date: date
    open: float
    prev_close: Optional[float] = None


class SignalSnapshot:
    """
    Restricted market snapshot for signal calculation.
    
    CRITICAL: This class intentionally DOES NOT provide access to:
    - T-day close, settle, high, low, volume, oi
    - T-day index close
    
    This prevents lookahead bias in strategy development.
    """
    
    # Fields that are FORBIDDEN for signal calculation
    FORBIDDEN_FIELDS = {'close', 'settle', 'high', 'low', 'volume', 'amount', 'open_interest', 'oi_change'}
    
    def __init__(
        self,
        trade_date: date,
        index_bar: RestrictedIndexBar,
        futures_bars: Dict[str, RestrictedFuturesBar],
    ):
        self.trade_date = trade_date
        self.index_bar = index_bar
        self.futures_bars = futures_bars
    
    def __repr__(self) -> str:
        return f"SignalSnapshot({self.trade_date}, contracts={len(self.futures_bars)})"
    
    def get_futures_price(
        self,
        ts_code: str,
        field: Literal['open', 'pre_settle'] = 'open'
    ) -> Optional[float]:
        """
        Get futures price. Only 'open' and 'pre_settle' are allowed.
        """
        if field not in ('open', 'pre_settle'):
            logger.warning(f"SignalSnapshot: field '{field}' not available, using 'open'")
            field = 'open'
        
        bar = self.futures_bars.get(ts_code)
        if bar is None:
            return None
        return getattr(bar, field, None)
    
    def get_index_price(self, field: Literal['open', 'prev_close'] = 'open') -> Optional[float]:
        """
        Get index price. Only 'open' and 'prev_close' are allowed.
        """
        if field == 'open':
            return self.index_bar.open
        elif field == 'prev_close':
            return self.index_bar.prev_close
        else:
            logger.warning(f"SignalSnapshot: index field '{field}' not available")
            return None
    
    def get_basis(
        self,
        ts_code: str,
        relative: bool = True,
        use_prev_close: bool = False
    ) -> Optional[float]:
        """
        Calculate basis using only available data.
        
        Args:
            ts_code: Contract code
            relative: If True, return (F - S) / S
            use_prev_close: If True, use prev_close for both futures and index
                           If False, use open prices
        """
        bar = self.futures_bars.get(ts_code)
        if bar is None:
            return None
        
        if use_prev_close:
            futures_price = bar.prev_settle or bar.pre_settle
            spot_price = self.index_bar.prev_close
        else:
            futures_price = bar.open
            spot_price = self.index_bar.open
        
        if futures_price is None or futures_price <= 0:
            return None
        if spot_price is None or spot_price <= 0:
            return None
        
        if relative:
            return (futures_price - spot_price) / spot_price
        else:
            return futures_price - spot_price
    
    def get_prev_volume(self, ts_code: str) -> Optional[float]:
        """Get previous day's volume."""
        bar = self.futures_bars.get(ts_code)
        return bar.prev_volume if bar else None
    
    def get_prev_oi(self, ts_code: str) -> Optional[float]:
        """Get previous day's open interest."""
        bar = self.futures_bars.get(ts_code)
        return bar.prev_oi if bar else None
    
    def get_available_contracts(self) -> list:
        """Get list of available contract codes."""
        return list(self.futures_bars.keys())


class SnapshotFactory:
    """
    Factory to create restricted snapshots from full market data.
    """
    
    @staticmethod
    def create_signal_snapshot(
        trade_date: date,
        index_bar: IndexDailyBar,
        futures_quotes: Dict[str, FuturesDailyBar],
        prev_index_bar: Optional[IndexDailyBar] = None,
        prev_futures_quotes: Optional[Dict[str, FuturesDailyBar]] = None,
    ) -> SignalSnapshot:
        """
        Create a SignalSnapshot from full market data.
        
        Args:
            trade_date: Current trading date
            index_bar: Current day's index bar
            futures_quotes: Current day's futures bars
            prev_index_bar: Previous day's index bar (for prev_close)
            prev_futures_quotes: Previous day's futures bars (for prev data)
        """
        # Create restricted index bar
        restricted_index = RestrictedIndexBar(
            trade_date=trade_date,
            open=index_bar.open,
            prev_close=prev_index_bar.close if prev_index_bar else None,
        )
        
        # Create restricted futures bars
        restricted_futures = {}
        for ts_code, bar in futures_quotes.items():
            prev_bar = prev_futures_quotes.get(ts_code) if prev_futures_quotes else None
            
            restricted_futures[ts_code] = RestrictedFuturesBar(
                trade_date=trade_date,
                ts_code=ts_code,
                open=bar.open,
                pre_settle=bar.pre_settle,
                prev_close=prev_bar.close if prev_bar else None,
                prev_settle=prev_bar.settle if prev_bar else None,
                prev_volume=prev_bar.volume if prev_bar else None,
                prev_oi=prev_bar.open_interest if prev_bar else None,
            )
        
        return SignalSnapshot(
            trade_date=trade_date,
            index_bar=restricted_index,
            futures_bars=restricted_futures,
        )
