"""
Backtest engine - drives the simulation.
"""
from dataclasses import dataclass
from datetime import date
from typing import Optional, Dict
import pandas as pd
from loguru import logger

from ..data.handler import DataHandler
from ..data.signal_snapshot import SignalSnapshot
from ..strategy.base import Strategy
from ..account.account import Account
from ..config import TRADING_DAYS_PER_YEAR
from .analyzer import Analyzer


@dataclass
class BacktestResult:
    """Container for backtest results."""
    nav_series: pd.Series
    benchmark_nav: pd.Series
    trade_summary: pd.DataFrame
    metrics: Dict[str, float]
    analyzer: Analyzer


class BacktestEngine:
    """
    Backtest engine that drives the simulation.
    
    Flow for each trading day:
    1. Get market snapshot
    2. Mark-to-market existing positions
    3. Call strategy to get target positions
    4. Execute trades to reach target
    5. Record NAV
    """
    
    def __init__(
        self,
        data_handler: DataHandler,
        strategy: Strategy,
        initial_capital: float = 10_000_000.0,
        margin_rate: float = 0.12,
        commission_rate: float = 0.00023,
        strategy_name: str = "Strategy",
        benchmark_name: str = "Benchmark",
        risk_free_rate: float = 0.02,
        trading_days_per_year: int = TRADING_DAYS_PER_YEAR,
        signal_price_field: str = "open",
        execution_price_field: str = "open",
    ):
        self.data_handler = data_handler
        self.strategy = strategy
        self.initial_capital = initial_capital
        self.margin_rate = margin_rate
        self.commission_rate = commission_rate
        self.strategy_name = strategy_name
        self.benchmark_name = benchmark_name
        self.risk_free_rate = risk_free_rate
        self.trading_days_per_year = trading_days_per_year
        self.signal_price_field = signal_price_field
        self.execution_price_field = execution_price_field
        
        self.account: Optional[Account] = None
        self.analyzer: Optional[Analyzer] = None
    
    def run(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        verbose: bool = True
    ) -> BacktestResult:
        """
        Run the backtest.
        
        Args:
            start_date: Start date (None = use first available)
            end_date: End date (None = use last available)
            verbose: Whether to print progress
            
        Returns:
            BacktestResult containing NAV series, metrics, etc.
        """
        # Initialize account
        self.account = Account(
            initial_capital=self.initial_capital,
            margin_rate=self.margin_rate,
            commission_rate=self.commission_rate,
            execution_price_field=self.execution_price_field,
        )
        
        # Get trading calendar
        calendar = self.data_handler.get_trading_calendar(start_date, end_date)
        
        if not calendar:
            raise ValueError("No trading days in the specified range")
        
        if verbose:
            logger.info(f"Running backtest from {calendar[0]} to {calendar[-1]}")
            logger.info(f"Total trading days: {len(calendar)}")
        
        # Get contract lookup for trade execution
        contracts = self.data_handler.contract_chain.contracts
        
        # Main backtest loop
        for i, trade_date in enumerate(calendar):
            self._process_day(trade_date, contracts)
            
            if verbose and (i + 1) % 100 == 0:
                logger.info(f"Processed {i + 1}/{len(calendar)} days, NAV: {self.account.nav:.4f}")
        
        # Build analyzer
        benchmark_nav = self.data_handler.index.get_nav_series(start_date, end_date)
        
        self.analyzer = Analyzer(
            nav_series=self.account.get_nav_series(),
            benchmark_nav=benchmark_nav,
            trade_log=self.account.trade_log,
            strategy_name=self.strategy_name,
            benchmark_name=self.benchmark_name,
            risk_free_rate=self.risk_free_rate,
            trading_days_per_year=self.trading_days_per_year,
        )
        
        # Compute metrics
        metrics = self.analyzer.compute_metrics()
        
        if verbose:
            logger.info(f"Backtest completed. Final NAV: {self.account.nav:.4f}")
            self._print_summary(metrics)
        
        return BacktestResult(
            nav_series=self.account.get_nav_series(),
            benchmark_nav=benchmark_nav,
            trade_summary=self.account.get_trade_summary(),
            metrics=metrics,
            analyzer=self.analyzer,
        )
    
    def _process_day(self, trade_date: date, contracts: dict) -> None:
        """
        Process a single trading day.
        
        Timeline:
        1. Get full snapshot (for settlement and execution)
        2. Get signal snapshot (RESTRICTED - for strategy)
        3. Mark-to-market using settle price
        4. Strategy generates signal using ONLY signal snapshot
        5. Execute trades at execution price
        6. Record NAV
        """
        # Get full snapshot (for mark-to-market and execution)
        full_snapshot = self.data_handler.get_snapshot(trade_date)
        if full_snapshot is None:
            return
        
        # Get RESTRICTED signal snapshot (prevents lookahead bias)
        signal_snapshot = self.data_handler.get_signal_snapshot(trade_date)
        if signal_snapshot is None:
            return
        
        # Mark-to-market existing positions (uses settle price - correct)
        self.account.mark_to_market(full_snapshot)
        
        # Get target positions from strategy using RESTRICTED snapshot
        # Strategy CANNOT see today's close, settle, volume, etc.
        target_positions = self.strategy.on_bar(signal_snapshot, self.account)
        
        # Execute trades using full snapshot (at configured execution price)
        self.account.rebalance_to_target(
            target_positions,
            full_snapshot,
            contracts,
            reason="STRATEGY"
        )
        
        # Record NAV
        self.account.record_nav(trade_date)
    
    def _print_summary(self, metrics: Dict[str, float]) -> None:
        """Print backtest summary."""
        logger.info("=" * 50)
        logger.info("Backtest Summary")
        logger.info("=" * 50)
        logger.info(f"Annualized Return: {metrics.get('annualized_return', 0):.2%}")
        logger.info(f"Annualized Volatility: {metrics.get('annualized_volatility', 0):.2%}")
        logger.info(f"Sharpe Ratio: {metrics.get('sharpe_ratio', 0):.2f}")
        logger.info(f"Max Drawdown: {metrics.get('max_drawdown', 0):.2%}")
        logger.info(f"Benchmark Return: {metrics.get('benchmark_return', 0):.2%}")
        logger.info(f"Alpha (Excess Return): {metrics.get('alpha', 0):.2%}")
        logger.info(f"Total Trades: {metrics.get('total_trades', 0):.0f}")
        logger.info("=" * 50)
