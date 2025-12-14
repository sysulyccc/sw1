from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Dict, Optional

import pandas as pd

from ..account.account import Account
from ..data.signal_snapshot import SignalSnapshot
from ..domain.contract import FuturesContract


class NavTracker(ABC):
    @abstractmethod
    def reset(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def on_pre_trade(
        self,
        signal_snapshot: SignalSnapshot,
        account: Account,
        target_positions: Dict[str, int],
        contracts: Dict[str, FuturesContract],
        execution_price_field: str,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def on_post_trade(self, total_commission: float) -> None:
        raise NotImplementedError

    @abstractmethod
    def on_settlement(self, trade_date: date, daily_pnl: float) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_nav_series(self, default_nav_series: pd.Series) -> pd.Series:
        raise NotImplementedError

    @abstractmethod
    def get_nav_for_date(self, trade_date: date, default_nav: float) -> float:
        raise NotImplementedError


class NullNavTracker(NavTracker):
    def reset(self) -> None:
        return

    def on_pre_trade(
        self,
        signal_snapshot: SignalSnapshot,
        account: Account,
        target_positions: Dict[str, int],
        contracts: Dict[str, FuturesContract],
        execution_price_field: str,
    ) -> None:
        return

    def on_post_trade(self, total_commission: float) -> None:
        return

    def on_settlement(self, trade_date: date, daily_pnl: float) -> None:
        return

    def get_nav_series(self, default_nav_series: pd.Series) -> pd.Series:
        return default_nav_series

    def get_nav_for_date(self, trade_date: date, default_nav: float) -> float:
        return default_nav


class FixedLotNormalizedNavTracker(NavTracker):
    def __init__(self):
        self._notional_base: Optional[float] = None
        self._equity: Optional[float] = None
        self._nav_history: Dict[date, float] = {}
        self._pending_price_move_pnl: float = 0.0

    @property
    def notional_base(self) -> Optional[float]:
        return self._notional_base

    @property
    def nav_history(self) -> Dict[date, float]:
        return self._nav_history

    def reset(self) -> None:
        self._notional_base = None
        self._equity = None
        self._nav_history = {}
        self._pending_price_move_pnl = 0.0

    def on_pre_trade(
        self,
        signal_snapshot: SignalSnapshot,
        account: Account,
        target_positions: Dict[str, int],
        contracts: Dict[str, FuturesContract],
        execution_price_field: str,
    ) -> None:
        self._pending_price_move_pnl = 0.0

        ts_codes = set(target_positions.keys()).union(set(account.positions.keys()))
        for ts_code in ts_codes:
            current_volume = account.get_position_volume(ts_code)
            target_volume = target_positions.get(ts_code, 0)
            delta = target_volume - current_volume
            if delta == 0:
                continue

            position = account.get_position(ts_code)
            if position is None:
                continue

            price = signal_snapshot.get_futures_price(ts_code, execution_price_field)
            if price is None:
                price = position.last_settle
            if price is None:
                continue

            self._pending_price_move_pnl += (price - position.last_settle) * position.volume * position.multiplier

        if self._notional_base is not None and self._notional_base > 0:
            return

        for ts_code, vol in target_positions.items():
            if vol == 0:
                continue

            contract = contracts.get(ts_code)
            if contract is None:
                continue

            price = signal_snapshot.get_futures_price(ts_code, execution_price_field)
            if price is None or price <= 0:
                continue

            base = abs(vol) * price * contract.multiplier
            if base <= 0:
                continue

            self._notional_base = base
            self._equity = base
            return

    def on_post_trade(self, total_commission: float) -> None:
        if self._equity is None:
            return
        self._equity += self._pending_price_move_pnl
        self._equity -= total_commission
        self._pending_price_move_pnl = 0.0

    def on_settlement(self, trade_date: date, daily_pnl: float) -> None:
        if self._notional_base is None or self._notional_base <= 0:
            self._nav_history[trade_date] = 1.0
            return

        if self._equity is None:
            self._equity = self._notional_base

        self._equity += daily_pnl
        self._nav_history[trade_date] = self._equity / self._notional_base

    def get_nav_series(self, default_nav_series: pd.Series) -> pd.Series:
        dates = sorted(self._nav_history.keys())
        if not dates:
            return default_nav_series
        values = [self._nav_history[d] for d in dates]
        return pd.Series(values, index=pd.DatetimeIndex(dates))

    def get_nav_for_date(self, trade_date: date, default_nav: float) -> float:
        return self._nav_history.get(trade_date, default_nav)


def create_nav_tracker(strategy) -> NavTracker:
    if getattr(strategy, "position_mode", None) == "fixed_lot":
        return FixedLotNormalizedNavTracker()
    return NullNavTracker()
