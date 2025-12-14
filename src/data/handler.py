"""
Data handler - unified data access interface.
"""
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import polars as pl
from loguru import logger

from ..domain.bars import IndexDailyBar, FuturesDailyBar
from ..domain.index import EquityIndex
from ..domain.contract import FuturesContract
from ..domain.chain import ContractChain
from .snapshot import MarketSnapshot
from .signal_snapshot import SignalSnapshot, SnapshotFactory


# Mapping from fut_code to index info
FUT_TO_INDEX = {
    "IC": ("000905.SH", "CSI500"),
    "IM": ("000852.SH", "CSI1000"),
    "IF": ("000300.SH", "CSI300"),
}


class DataHandler:
    """
    Unified data access interface.
    Builds domain objects from processed parquet files.
    Provides snapshots and trading calendar to backtest engine.
    """
    
    def __init__(
        self,
        index: EquityIndex,
        contract_chain: ContractChain,
        calendar: List[date],
        margin_rates: Optional[Dict[Tuple[str, date], float]] = None
    ):
        self.index = index
        self.contract_chain = contract_chain
        self.calendar = calendar
        self._margin_rates = margin_rates or {}
        self._snapshot_cache: Dict[date, MarketSnapshot] = {}
        self._signal_snapshot_cache: Dict[date, SignalSnapshot] = {}
    
    def __repr__(self) -> str:
        return f"DataHandler({self.contract_chain.fut_code}, calendar={len(self.calendar)} days)"
    
    @classmethod
    def from_processed_data(
        cls,
        data_path: str,
        fut_code: str
    ) -> "DataHandler":
        """
        Build DataHandler from processed parquet files.
        Args:
            data_path: Path to processed_data directory
            fut_code: Futures code ('IC', 'IM', or 'IF')
        """
        data_path = Path(data_path)
        
        if fut_code not in FUT_TO_INDEX:
            raise ValueError(f"Unknown fut_code: {fut_code}. Must be one of {list(FUT_TO_INDEX.keys())}")
        
        index_code, index_name = FUT_TO_INDEX[fut_code]
        
        # Load index data
        index = cls._load_index(data_path, index_code, index_name)
        logger.info(f"Loaded index {index_name}: {len(index.daily_bars)} bars")
        
        # Load contract info
        contracts = cls._load_contracts(data_path, fut_code)
        logger.info(f"Loaded {len(contracts)} {fut_code} contracts")
        
        # Load futures daily bars
        cls._load_futures_bars(data_path, fut_code, contracts)
        
        # Build contract chain
        contract_chain = ContractChain(index, fut_code, contracts)
        
        # Build trading calendar (intersection of index and futures dates)
        index_dates = set(index.get_trading_dates())
        futures_dates = set()
        for contract in contracts.values():
            futures_dates.update(contract.get_trading_dates())
        
        calendar = sorted(index_dates & futures_dates)
        logger.info(f"Trading calendar: {calendar[0]} to {calendar[-1]}, {len(calendar)} days")

        contract_chain.set_trading_calendar(calendar)
        
        # Load margin rates
        margin_rates = cls._load_margin_rates(data_path, fut_code)
        
        return cls(index, contract_chain, calendar, margin_rates)
    
    @staticmethod
    def _load_index(
        data_path: Path,
        index_code: str,
        index_name: str
    ) -> EquityIndex:
        """Load index from parquet file."""
        index_file = data_path / "index" / f"{index_name}_daily.parquet"
        df = pl.read_parquet(index_file)
        
        index = EquityIndex(index_code, index_name)
        
        for row in df.iter_rows(named=True):
            bar = IndexDailyBar(
                trade_date=row["trade_date"],
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
            )
            index.add_bar(bar)
        
        return index
    
    @staticmethod
    def _load_contracts(
        data_path: Path,
        fut_code: str
    ) -> Dict[str, FuturesContract]:
        """Load contract info from parquet file."""
        info_file = data_path / "contracts" / f"{fut_code}_info.parquet"
        df = pl.read_parquet(info_file)
        
        contracts = {}
        for row in df.iter_rows(named=True):
            contract = FuturesContract(
                ts_code=row["ts_code"],
                fut_code=row["fut_code"],
                multiplier=row["multiplier"],
                list_date=row["list_date"],
                delist_date=row["delist_date"],
                last_ddate=row["last_ddate"],
                name=row.get("name"),
            )
            contracts[contract.ts_code] = contract
        
        return contracts
    
    @staticmethod
    def _load_futures_bars(
        data_path: Path,
        fut_code: str,
        contracts: Dict[str, FuturesContract]
    ) -> None:
        """Load futures daily bars and attach to contracts."""
        bars_file = data_path / "futures" / f"{fut_code}_daily.parquet"
        df = pl.read_parquet(bars_file)
        
        bar_count = 0
        for row in df.iter_rows(named=True):
            ts_code = row["ts_code"]
            if ts_code not in contracts:
                continue
            
            # Use close as fallback for settle when settle is None or 0
            close_price = row["close"] or 0.0
            settle_price = row["settle"] if row["settle"] else close_price
            pre_settle_price = row["pre_settle"] if row["pre_settle"] else close_price
            
            bar = FuturesDailyBar(
                trade_date=row["trade_date"],
                open=row["open"] or 0.0,
                high=row["high"] or 0.0,
                low=row["low"] or 0.0,
                close=close_price,
                settle=settle_price,
                pre_settle=pre_settle_price,
                volume=row["volume"] or 0.0,
                amount=row["amount"] or 0.0,
                open_interest=row["open_interest"] or 0.0,
                oi_change=row["oi_change"],
            )
            contracts[ts_code].add_bar(bar)
            bar_count += 1
        
        logger.info(f"Loaded {bar_count} {fut_code} daily bars")
    
    @staticmethod
    def _load_margin_rates(
        data_path: Path,
        fut_code: str
    ) -> Dict[Tuple[str, date], float]:
        """Load margin ratio history."""
        margin_file = data_path / "margin" / "margin_ratio.parquet"
        if not margin_file.exists():
            return {}
        
        df = pl.read_parquet(margin_file)
        df = df.filter(pl.col("fut_code") == fut_code)
        
        margin_rates = {}
        for row in df.iter_rows(named=True):
            key = (row["fut_code"], row["trade_date"])
            margin_rates[key] = row["long_margin_ratio"] / 100.0
        
        return margin_rates
    
    def get_trading_calendar(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> List[date]:
        """Get trading calendar within date range."""
        dates = self.calendar
        
        if start_date:
            dates = [d for d in dates if d >= start_date]
        if end_date:
            dates = [d for d in dates if d <= end_date]
        
        return dates
    
    def get_snapshot(self, trade_date: date) -> Optional[MarketSnapshot]:
        """
        Get market snapshot for a specific date.
        Results are cached for performance.
        """
        if trade_date in self._snapshot_cache:
            return self._snapshot_cache[trade_date]
        
        index_bar = self.index.get_bar(trade_date)
        if index_bar is None:
            return None
        
        futures_quotes = self.contract_chain.get_chain_snapshot(trade_date)
        if not futures_quotes:
            return None
        
        snapshot = MarketSnapshot(trade_date, index_bar, futures_quotes)
        self._snapshot_cache[trade_date] = snapshot
        
        return snapshot
    
    def get_contract_chain(self) -> ContractChain:
        """Get the contract chain."""
        return self.contract_chain
    
    def get_index(self) -> EquityIndex:
        """Get the underlying index."""
        return self.index
    
    def get_margin_rate(
        self,
        trade_date: date,
        default: float = 0.12
    ) -> float:
        """
        Get margin rate for a specific date.
        Returns default if not found.
        """
        key = (self.contract_chain.fut_code, trade_date)
        return self._margin_rates.get(key, default)
    
    def get_prev_trading_date(self, trade_date: date) -> Optional[date]:
        """Get the previous trading date."""
        try:
            idx = self.calendar.index(trade_date)
            if idx > 0:
                return self.calendar[idx - 1]
        except ValueError:
            pass
        return None
    
    def get_next_trading_date(self, trade_date: date) -> Optional[date]:
        """Get the next trading date."""
        try:
            idx = self.calendar.index(trade_date)
            if idx < len(self.calendar) - 1:
                return self.calendar[idx + 1]
        except ValueError:
            pass
        return None
    
    def get_signal_snapshot(self, trade_date: date) -> Optional[SignalSnapshot]:
        """
        Get RESTRICTED market snapshot for signal calculation.
        
        This snapshot intentionally DOES NOT provide:
        - T-day close, settle, high, low, volume, oi
        - T-day index close
        
        Only provides:
        - T-day open, pre_settle
        - T-day index open
        - T-1 day complete data
        """
        if trade_date in self._signal_snapshot_cache:
            return self._signal_snapshot_cache[trade_date]
        
        # Get current day data
        index_bar = self.index.get_bar(trade_date)
        if index_bar is None:
            return None
        
        futures_quotes = self.contract_chain.get_chain_snapshot(trade_date)
        if not futures_quotes:
            return None
        
        # Get previous day data
        prev_date = self.get_prev_trading_date(trade_date)
        prev_index_bar = self.index.get_bar(prev_date) if prev_date else None
        prev_futures_quotes = self.contract_chain.get_chain_snapshot(prev_date) if prev_date else None
        
        # Create restricted snapshot
        signal_snapshot = SnapshotFactory.create_signal_snapshot(
            trade_date=trade_date,
            index_bar=index_bar,
            futures_quotes=futures_quotes,
            prev_index_bar=prev_index_bar,
            prev_futures_quotes=prev_futures_quotes,
        )
        
        self._signal_snapshot_cache[trade_date] = signal_snapshot
        return signal_snapshot
