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
from .nav_tracker import NavTracker, create_nav_tracker


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
        use_dynamic_margin: bool = False,
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
        self.use_dynamic_margin = use_dynamic_margin
        
        self.account: Optional[Account] = None
        self.analyzer: Optional[Analyzer] = None

        self._nav_tracker: Optional[NavTracker] = None

    def _ensure_nav_tracker(self) -> NavTracker:
        desired = create_nav_tracker(self.strategy)
        if self._nav_tracker is None or type(self._nav_tracker) is not type(desired):
            self._nav_tracker = desired
        return self._nav_tracker
    
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

        nav_tracker = self._ensure_nav_tracker()
        nav_tracker.reset()
        
        # Main backtest loop
        for i, trade_date in enumerate(calendar):
            self._process_day(trade_date, contracts)
            
            if verbose and (i + 1) % 100 == 0:
                nav_for_log = nav_tracker.get_nav_for_date(trade_date, self.account.nav)
                logger.info(f"Processed {i + 1}/{len(calendar)} days, NAV: {nav_for_log:.4f}")
        
        # Build analyzer
        benchmark_nav = self.data_handler.index.get_nav_series(start_date, end_date)

        nav_series = nav_tracker.get_nav_series(self.account.get_nav_series())
        
        self.analyzer = Analyzer(
            nav_series=nav_series,
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
            final_nav_for_log = nav_tracker.get_nav_for_date(calendar[-1], self.account.nav)
            logger.info(f"Backtest completed. Final NAV: {final_nav_for_log:.4f}")
            self._print_summary(metrics)
        
        return BacktestResult(
            nav_series=nav_series,
            benchmark_nav=benchmark_nav,
            trade_summary=self.account.get_trade_summary(),
            metrics=metrics,
            analyzer=self.analyzer,
        )
    
    def _process_day(self, trade_date: date, contracts: dict) -> None:
        """
        Process a single trading day with correct timeline.
        
        ============ TIMELINE ============
        
        T Day 09:30 Open:
        ├─ Get SignalSnapshot (Only open, pre_settle, T-1 data)
        ├─ Strategy generates signal (CANNOT see T-day close/settle/volume)
        └─ Execute trades @ open price
        
        T Day 15:00 Close:
        ├─ Settlement price published
        ├─ Mark-to-market: PnL = (settle(T) - settle(T-1)) * volume
        └─ Record NAV (valued at settle)
        
        ==================================
        """
        # ============ Open (09:30) ============
        
        # Update margin rate if dynamic
        if self.use_dynamic_margin:
            # Get today's margin rate, fallback to default self.margin_rate
            today_margin = self.data_handler.get_margin_rate(trade_date, default=self.margin_rate)
            self.account.margin_rate = today_margin
        
        # Get RESTRICTED signal snapshot - strategy can ONLY see this
        signal_snapshot = self.data_handler.get_signal_snapshot(trade_date)
        if signal_snapshot is None:
            return
        
        # Strategy generates signal using ONLY SignalSnapshot
        # CANNOT access T-day close, settle, volume, oi, index close
        target_positions = self.strategy.on_bar(signal_snapshot, self.account)

        nav_tracker = self._ensure_nav_tracker()
        nav_tracker.on_pre_trade(
            signal_snapshot=signal_snapshot,
            account=self.account,
            target_positions=target_positions,
            contracts=contracts,
            execution_price_field=self.execution_price_field,
        )
        
        # Execute trades at open price
        # Note: We need execution prices from signal_snapshot (open/pre_settle)
        total_commission = self.account.rebalance_to_target(
            target_positions,
            signal_snapshot,  # Use SignalSnapshot for execution too (SOLID principle)
            contracts,
            reason="STRATEGY"
        )

        nav_tracker.on_post_trade(total_commission)
        
        # ============ Close (15:00 后) ============
        
        # Get full snapshot for settlement (now settle is known)
        full_snapshot = self.data_handler.get_snapshot(trade_date)
        if full_snapshot is None:
            return
        
        # Mark-to-market: settle today's PnL using T-day settle price
        # PnL = (settle(T) - settle(T-1)) * volume * multiplier
        daily_pnl = self.account.mark_to_market(full_snapshot)
        
        # Record NAV (valued at settle price)
        self.account.record_nav(trade_date)

        nav_tracker.on_settlement(trade_date, daily_pnl)
    
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
