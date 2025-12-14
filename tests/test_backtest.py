"""
Tests for backtest layer (Layer 5).
"""
import pytest
from datetime import date
from pathlib import Path

from src.data.handler import DataHandler
from src.strategy.baseline_roll import BaselineRollStrategy
from src.backtest.engine import BacktestEngine
from src.backtest.analyzer import Analyzer


class TestBacktestEngine:
    """Tests for BacktestEngine."""
    
    @pytest.fixture
    def data_handler(self):
        data_path = Path("/root/sw1/processed_data")
        if not data_path.exists():
            pytest.skip("Processed data not available")
        return DataHandler.from_processed_data(str(data_path), "IC")
    
    @pytest.fixture
    def strategy(self, data_handler):
        return BaselineRollStrategy(
            contract_chain=data_handler.contract_chain,
            roll_days_before_expiry=2,
            contract_selection='nearby',
            target_leverage=1.0,
        )
    
    def test_run_short_backtest(self, data_handler, strategy):
        engine = BacktestEngine(
            data_handler=data_handler,
            strategy=strategy,
            initial_capital=10_000_000.0,
        )
        
        # Run for short period
        result = engine.run(
            start_date=date(2020, 1, 1),
            end_date=date(2020, 6, 30),
            verbose=False,
        )
        
        # Check result structure
        assert result.nav_series is not None
        assert len(result.nav_series) > 0
        assert 'annualized_return' in result.metrics
        assert 'max_drawdown' in result.metrics


class TestAnalyzer:
    """Tests for Analyzer."""
    
    @pytest.fixture
    def sample_data(self):
        import pandas as pd
        import numpy as np
        
        dates = pd.date_range('2020-01-01', '2020-12-31', freq='B')
        n = len(dates)
        
        # Simulate NAV series with trend and noise
        np.random.seed(42)
        returns = np.random.randn(n) * 0.01 + 0.0003  # ~8% annual return
        nav = (1 + pd.Series(returns)).cumprod()
        nav_series = pd.Series(nav.values, index=dates)
        
        # Benchmark with slightly lower return
        bench_returns = np.random.randn(n) * 0.01 + 0.0002
        bench = (1 + pd.Series(bench_returns)).cumprod()
        benchmark_nav = pd.Series(bench.values, index=dates)
        
        return nav_series, benchmark_nav
    
    def test_compute_metrics(self, sample_data):
        nav_series, benchmark_nav = sample_data
        
        analyzer = Analyzer(nav_series, benchmark_nav)
        metrics = analyzer.compute_metrics()
        
        assert 'annualized_return' in metrics
        assert 'sharpe_ratio' in metrics
        assert 'max_drawdown' in metrics
        assert 'alpha' in metrics
    
    def test_generate_report(self, sample_data):
        nav_series, benchmark_nav = sample_data
        
        analyzer = Analyzer(nav_series, benchmark_nav)
        report = analyzer.generate_report()
        
        assert "PERFORMANCE REPORT" in report
        assert "Sharpe Ratio" in report
    
    def test_custom_trading_days(self, sample_data):
        """Test with China's 242 trading days."""
        nav_series, benchmark_nav = sample_data
        
        analyzer = Analyzer(
            nav_series,
            benchmark_nav,
            trading_days_per_year=242
        )
        metrics = analyzer.compute_metrics()
        
        # Metrics should be computed with 242 days
        assert metrics['trading_days'] == len(nav_series)
    
    def test_strategy_and_benchmark_names(self, sample_data):
        """Test custom names appear in report."""
        nav_series, benchmark_nav = sample_data
        
        analyzer = Analyzer(
            nav_series,
            benchmark_nav,
            strategy_name="My Strategy",
            benchmark_name="CSI 500",
        )
        report = analyzer.generate_report()
        
        assert "My Strategy" in report
        assert "CSI 500" in report
    
    def test_get_metrics_dataframe(self, sample_data):
        """Test metrics DataFrame output."""
        nav_series, benchmark_nav = sample_data
        
        analyzer = Analyzer(nav_series, benchmark_nav)
        df = analyzer.get_metrics_dataframe()
        
        assert len(df) > 0
        assert 'Metric' in df.columns
        assert 'Value' in df.columns
    
    def test_export_trade_log(self, sample_data, tmp_path):
        """Test trade log export."""
        from src.account.account import TradeRecord
        
        nav_series, benchmark_nav = sample_data
        trade_log = [
            TradeRecord(
                trade_date=date(2020, 1, 2),
                ts_code="IC2001.CFX",
                direction="BUY",
                volume=10,
                price=5000.0,
                amount=10_000_000.0,
                commission=2300.0,
                reason="OPEN",
            )
        ]
        
        analyzer = Analyzer(nav_series, benchmark_nav, trade_log=trade_log)
        
        output_path = tmp_path / "trade_log.csv"
        analyzer.export_trade_log(output_path)
        
        assert output_path.exists()
        import pandas as pd
        df = pd.read_csv(output_path)
        assert len(df) == 1
        assert df.iloc[0]['ts_code'] == "IC2001.CFX"

    def test_save_all_generates_trade_log_plot(self, sample_data, tmp_path):
        from src.account.account import TradeRecord

        nav_series, benchmark_nav = sample_data
        trade_log = [
            TradeRecord(
                trade_date=date(2020, 1, 2),
                ts_code="IC2001.CFX",
                direction="BUY",
                volume=10,
                price=5000.0,
                amount=10_000_000.0,
                commission=2300.0,
                reason="OPEN",
            ),
            TradeRecord(
                trade_date=date(2020, 2, 3),
                ts_code="IC2001.CFX",
                direction="SELL",
                volume=5,
                price=5100.0,
                amount=5_100_000.0,
                commission=1173.0,
                reason="REBALANCE",
            ),
        ]

        analyzer = Analyzer(nav_series, benchmark_nav, trade_log=trade_log, strategy_name="S")
        analyzer.save_all(output_dir=tmp_path, run_name="run", dpi=50, fmt="png")

        assert (tmp_path / "run" / "trade_log.png").exists()


class TestConfig:
    """Tests for configuration."""
    
    def test_load_default_config(self):
        from src.config import load_config, TRADING_DAYS_PER_YEAR
        
        config = load_config()
        
        assert config.data.fut_code == "IC"
        assert config.backtest.trading_days_per_year == TRADING_DAYS_PER_YEAR
    
    def test_trading_days_constant(self):
        from src.config import TRADING_DAYS_PER_YEAR
        
        assert TRADING_DAYS_PER_YEAR == 242


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
