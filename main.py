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
from src.strategy.BasisTimingRollStrategy import BasisTimingRollStrategy
from src.strategy.SpreadTimingRollStrategy import SpreadTimingRollStrategy
from src.strategy.LiquidityRollStrategy import LiquidityRollStrategy
from src.strategy.AERYRollStrategy import AERYRollStrategy
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
    if cfg.strategy.strategy_type == "baseline":
        strategy = BaselineRollStrategy(
            contract_chain=data_handler.contract_chain,
            roll_days_before_expiry=cfg.strategy.roll_days_before_expiry,
            contract_selection=cfg.strategy.contract_selection,
            target_leverage=cfg.strategy.target_leverage,
            min_roll_days=cfg.strategy.min_roll_days,
            signal_price_field=cfg.backtest.signal_price_field,
        )
    elif cfg.strategy.strategy_type == "basis_timing":
        strategy = BasisTimingRollStrategy(
            contract_chain=data_handler.contract_chain,
            roll_window_start=cfg.strategy.roll_days_before_expiry,
            history_window=cfg.strategy.min_roll_days,
            contract_selection=cfg.strategy.contract_selection,
            target_leverage=cfg.strategy.target_leverage,
            min_roll_days=cfg.strategy.min_roll_days,
            signal_price_field=cfg.backtest.signal_price_field,
        )
    elif cfg.strategy.strategy_type == "spread_timing":
        strategy = SpreadTimingRollStrategy(
            contract_chain=data_handler.contract_chain,
            roll_window_start=cfg.strategy.roll_days_before_expiry,
            history_window=cfg.strategy.min_roll_days,
            contract_selection=cfg.strategy.contract_selection,
            target_leverage=cfg.strategy.target_leverage,
            min_roll_days=cfg.strategy.min_roll_days,
            signal_price_field=cfg.backtest.signal_price_field,
        )
    elif cfg.strategy.strategy_type == "liquidity_roll":
        strategy = LiquidityRollStrategy(
            contract_chain=data_handler.contract_chain,
            roll_days_before_expiry=cfg.strategy.roll_days_before_expiry,
            contract_selection=cfg.strategy.contract_selection,
            target_leverage=cfg.strategy.target_leverage,
            min_roll_days=cfg.strategy.min_roll_days,
            signal_price_field=cfg.backtest.signal_price_field,
        )
    elif cfg.strategy.strategy_type == "aery_roll":
        strategy = AERYRollStrategy(
            contract_chain=data_handler.contract_chain,
            roll_days_before_expiry=cfg.strategy.roll_days_before_expiry,
            target_leverage=cfg.strategy.target_leverage,
            min_roll_days=cfg.strategy.min_roll_days,
            signal_price_field=cfg.backtest.signal_price_field,
        )
    else:
        raise ValueError(f"Unknown strategy type: {cfg.strategy.strategy_type}")
    
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
