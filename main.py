"""
Main entry point for the index enhancement strategy backtest.
Loads configuration from config.toml and runs the backtest.
"""
from datetime import date, datetime
from pathlib import Path
from typing import Optional
from loguru import logger

from src.config import Config, load_config
from src.data.handler import DataHandler
from src.strategy.baseline_roll import BaselineRollStrategy
from src.strategy.smart_roll import SmartRollStrategy
from src.strategy.basis_timing import BasisTimingStrategy
from src.strategy.fixed_lot_baseline_roll import FixedLotBaselineRollStrategy
from src.strategy.fixed_lot_smart_roll import FixedLotSmartRollStrategy
from src.strategy.fixed_lot_basis_timing import FixedLotBasisTimingStrategy
from src.backtest.engine import BacktestEngine


def parse_date(date_str: Optional[str]) -> Optional[date]:
    """Parse date string in YYYY-MM-DD format."""
    if not date_str:
        return None
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def run_backtest_from_config(config: Config):
    """
    Run backtest using configuration.
    """
    cfg = config
    
    logger.info(f"Loading data for {cfg.data.fut_code} ({cfg.data.index_name})")
    
    # Load data
    data_handler = DataHandler.from_processed_data(
        cfg.data.processed_data_path,
        cfg.data.fut_code
    )
    
    # Create strategy based on config
    # Note: fixed-lot is implemented as separate strategy classes.
    strategy_type = cfg.strategy.strategy_type
    is_fixed_lot = (
        getattr(cfg.strategy, "position_mode", "notional") == "fixed_lot"
        or str(strategy_type).endswith("_fixed_lot")
    )

    if strategy_type in ("baseline", "baseline_fixed_lot"):
        if is_fixed_lot:
            strategy = FixedLotBaselineRollStrategy(
                contract_chain=data_handler.contract_chain,
                roll_days_before_expiry=cfg.strategy.roll_days_before_expiry,
                contract_selection=cfg.strategy.contract_selection,
                fixed_lot_size=cfg.strategy.fixed_lot_size,
                min_roll_days=cfg.strategy.min_roll_days,
                signal_price_field=cfg.backtest.signal_price_field,
            )
        else:
            strategy = BaselineRollStrategy(
                contract_chain=data_handler.contract_chain,
                roll_days_before_expiry=cfg.strategy.roll_days_before_expiry,
                contract_selection=cfg.strategy.contract_selection,
                target_leverage=cfg.strategy.target_leverage,
                min_roll_days=cfg.strategy.min_roll_days,
                signal_price_field=cfg.backtest.signal_price_field,
            )
    elif strategy_type in ("smart_roll", "smart_roll_fixed_lot"):
        if is_fixed_lot:
            strategy = FixedLotSmartRollStrategy(
                contract_chain=data_handler.contract_chain,
                roll_days_before_expiry=cfg.strategy.roll_days_before_expiry,
                contract_selection=cfg.strategy.contract_selection,
                fixed_lot_size=cfg.strategy.fixed_lot_size,
                min_roll_days=cfg.strategy.min_roll_days,
                signal_price_field=cfg.backtest.signal_price_field,
                roll_criteria=cfg.strategy.roll_criteria,
                liquidity_threshold=cfg.strategy.liquidity_threshold,
            )
        else:
            strategy = SmartRollStrategy(
                contract_chain=data_handler.contract_chain,
                roll_days_before_expiry=cfg.strategy.roll_days_before_expiry,
                contract_selection=cfg.strategy.contract_selection,
                target_leverage=cfg.strategy.target_leverage,
                min_roll_days=cfg.strategy.min_roll_days,
                signal_price_field=cfg.backtest.signal_price_field,
                roll_criteria=cfg.strategy.roll_criteria,
                liquidity_threshold=cfg.strategy.liquidity_threshold,
            )
    elif strategy_type in ("basis_timing", "basis_timing_fixed_lot"):
        if is_fixed_lot:
            strategy = FixedLotBasisTimingStrategy(
                contract_chain=data_handler.contract_chain,
                roll_days_before_expiry=cfg.strategy.roll_days_before_expiry,
                contract_selection=cfg.strategy.contract_selection,
                fixed_lot_size=cfg.strategy.fixed_lot_size,
                min_roll_days=cfg.strategy.min_roll_days,
                signal_price_field=cfg.backtest.signal_price_field,
                basis_entry_threshold=cfg.strategy.basis_entry_threshold,
                basis_exit_threshold=cfg.strategy.basis_exit_threshold,
                lookback_window=cfg.strategy.lookback_window,
                use_percentile=cfg.strategy.use_percentile,
                entry_percentile=cfg.strategy.entry_percentile,
                exit_percentile=cfg.strategy.exit_percentile,
                basis_use_prev_close=cfg.strategy.basis_use_prev_close,
                neutral_hold_baseline=cfg.strategy.neutral_hold_baseline,
            )
        else:
            strategy = BasisTimingStrategy(
                contract_chain=data_handler.contract_chain,
                roll_days_before_expiry=cfg.strategy.roll_days_before_expiry,
                contract_selection=cfg.strategy.contract_selection,
                target_leverage=cfg.strategy.target_leverage,
                min_roll_days=cfg.strategy.min_roll_days,
                signal_price_field=cfg.backtest.signal_price_field,
                basis_entry_threshold=cfg.strategy.basis_entry_threshold,
                basis_exit_threshold=cfg.strategy.basis_exit_threshold,
                lookback_window=cfg.strategy.lookback_window,
                use_percentile=cfg.strategy.use_percentile,
                entry_percentile=cfg.strategy.entry_percentile,
                exit_percentile=cfg.strategy.exit_percentile,
                position_scale_by_basis=cfg.strategy.position_scale_by_basis,
                basis_use_prev_close=cfg.strategy.basis_use_prev_close,
                neutral_hold_baseline=cfg.strategy.neutral_hold_baseline,
            )
    else:
        raise ValueError(f"Unknown strategy type: {strategy_type}")
    
    # Determine margin rate
    margin_rate = cfg.account.default_margin_rate
    
    # Create backtest engine
    engine = BacktestEngine(
        data_handler=data_handler,
        strategy=strategy,
        initial_capital=cfg.account.initial_capital,
        margin_rate=margin_rate,
        commission_rate=cfg.account.commission_rate,
        strategy_name=cfg.strategy.strategy_name,
        benchmark_name=cfg.backtest.benchmark_name,
        risk_free_rate=cfg.backtest.risk_free_rate,
        trading_days_per_year=cfg.backtest.trading_days_per_year,
        signal_price_field=cfg.backtest.signal_price_field,
        execution_price_field=cfg.backtest.execution_price_field,
        use_dynamic_margin=cfg.account.use_dynamic_margin,
    )
    
    # Parse dates
    start_date = parse_date(cfg.backtest.start_date)
    end_date = parse_date(cfg.backtest.end_date)
    
    # Run backtest
    result = engine.run(start_date=start_date, end_date=end_date, verbose=True)
    
    # Save results
    if cfg.output.save_plots or cfg.output.save_trade_log or cfg.output.save_nav_series:
        run_name = f"{cfg.strategy.strategy_type}_{cfg.data.fut_code}"
        # Avoid overwriting notional results when running fixed-lot experiments
        if getattr(strategy, "position_mode", None) == "fixed_lot":
            run_name = f"{run_name}_fixed{cfg.strategy.fixed_lot_size}lot"
        result.analyzer.save_all(
            output_dir=cfg.output.output_path,
            run_name=run_name,
            dpi=cfg.output.plot_dpi,
            fmt=cfg.output.figure_format,
        )
        logger.info(f"Results saved to {cfg.output.output_path}/{run_name}/")
    
    return result


def main(config_path: Optional[str] = None):
    """Main function."""
    logger.info("=" * 60)
    logger.info("Index Enhancement Strategy Backtest")
    logger.info("=" * 60)
    
    # Load configuration
    config = load_config(config_path)
    logger.info(f"Loaded config: {config.strategy.strategy_type} on {config.data.fut_code}")
    
    # Run backtest
    result = run_backtest_from_config(config)
    
    # Print report
    print("\n" + result.analyzer.generate_report())
    
    return result


if __name__ == "__main__":
    import sys
    config_path = sys.argv[1] if len(sys.argv) > 1 else None
    main(config_path)
