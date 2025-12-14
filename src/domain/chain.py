"""
Contract chain - collection of all futures contracts for a specific underlying.
"""
from datetime import date
from bisect import bisect_left, bisect_right
from typing import Dict, List, Optional, Literal

from .index import EquityIndex
from .contract import FuturesContract
from .bars import FuturesDailyBar


class ContractChain:
    """
    Represents all futures contracts for a specific index (e.g., all IC contracts).
    Provides methods to query active contracts, main contract, etc.
    """

    def __init__(
        self,
        index: EquityIndex,
        fut_code: str,
        contracts: Optional[Dict[str, FuturesContract]] = None,
        trading_calendar: Optional[List[date]] = None,
    ):
        self.index = index
        self.fut_code = fut_code
        self._contracts: Dict[str, FuturesContract] = contracts or {}
        self._trading_calendar: Optional[List[date]] = trading_calendar

    def __repr__(self) -> str:
        return f"ContractChain({self.fut_code}, contracts={len(self._contracts)})"

    @property
    def contracts(self) -> Dict[str, FuturesContract]:
        return self._contracts

    def set_trading_calendar(self, calendar: List[date]) -> None:
        self._trading_calendar = calendar

    def _require_trading_calendar(self) -> List[date]:
        if not self._trading_calendar:
            raise ValueError("ContractChain trading_calendar is not set")
        return self._trading_calendar

    def get_last_trading_date(self, d: date) -> Optional[date]:
        calendar = self._require_trading_calendar()
        idx = bisect_left(calendar, d)
        if idx <= 0:
            return None
        return calendar[idx - 1]

    def trading_days_to_expiry(self, contract: FuturesContract, trade_date: date) -> int:
        calendar = self._require_trading_calendar()
        last_tradable_day = self.get_last_trading_date(contract.delist_date)
        if last_tradable_day is None:
            return 0

        start_idx = bisect_right(calendar, trade_date)
        end_idx = bisect_right(calendar, last_tradable_day)
        return max(end_idx - start_idx, 0)

    def add_contract(self, contract: FuturesContract) -> None:
        """Add a contract to the chain."""
        self._contracts[contract.ts_code] = contract

    def get_contract(self, ts_code: str) -> Optional[FuturesContract]:
        """Get a specific contract by ts_code."""
        return self._contracts.get(ts_code)

    def get_active_contracts(self, trade_date: date) -> List[FuturesContract]:
        """
        Get all contracts that are tradable on the given date.
        Returns: List of contracts sorted by expiry date (nearest first).
        """
        active = [
            c for c in self._contracts.values()
            if c.is_tradable(trade_date) and c.get_bar(trade_date) is not None
        ]
        return sorted(active, key=lambda c: c.delist_date)

    def get_nearby_contracts(
        self,
        trade_date: date,
        k: int = 2
    ) -> List[FuturesContract]:
        """
        Get the k nearest-to-expiry contracts.
        Args:
            trade_date: The date to query
            k: Number of contracts to return (default 2 for nearby + next)
        Returns: List of up to k contracts sorted by expiry date.
        """
        active = self.get_active_contracts(trade_date)
        return active[:k]

    def get_main_contract(
        self,
        trade_date: date,
        rule: Literal['volume', 'oi', 'nearby'] = 'volume'
    ) -> Optional[FuturesContract]:
        """
        Get the main contract based on selection rule.
        Args:
            trade_date: The date to query
            rule: Selection rule
                - 'volume': Highest trading volume
                - 'oi': Highest open interest
                - 'nearby': Nearest expiry
        Returns: The main contract or None if no active contracts.
        """
        active = self.get_active_contracts(trade_date)
        if not active:
            return None

        if rule == 'nearby':
            return active[0]
        elif rule == 'volume':
            return max(active, key=lambda c: c.get_volume(trade_date))
        elif rule == 'oi':
            return max(active, key=lambda c: c.get_open_interest(trade_date))
        else:
            return active[0]

    def get_chain_snapshot(
        self,
        trade_date: date
    ) -> Dict[str, FuturesDailyBar]:
        """
        Get all futures bars for the given date.
        Returns: Dict mapping ts_code to FuturesDailyBar.
        """
        snapshot = {}
        for ts_code, contract in self._contracts.items():
            bar = contract.get_bar(trade_date)
            if bar is not None:
                snapshot[ts_code] = bar
        return snapshot

    def get_all_contracts(self) -> List[FuturesContract]:
        """Get all contracts in the chain."""
        return list(self._contracts.values())

    def get_contracts_expiring_after(
        self,
        trade_date: date,
        min_days: int = 0
    ) -> List[FuturesContract]:
        """
        Get contracts that expire at least min_days after trade_date.
        Useful for finding roll target contracts.
        """
        active = self.get_active_contracts(trade_date)
        if self._trading_calendar:
            return [c for c in active if self.trading_days_to_expiry(c, trade_date) >= min_days]
        return [c for c in active if c.days_to_expiry(trade_date) >= min_days]
