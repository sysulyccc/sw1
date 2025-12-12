"""
Configuration management for the backtest system.
Loads settings from TOML config file.
"""
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Dict, Any
import tomllib
from loguru import logger


# China market trading days per year
TRADING_DAYS_PER_YEAR = 242

# Default paths
PROJECT_ROOT = Path(__file__).parent.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config.toml"
DEFAULT_DATA_PATH = PROJECT_ROOT / "processed_data"
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "output"


@dataclass
class DataConfig:
    """Data source configuration."""
    processed_data_path: str = str(DEFAULT_DATA_PATH)
    fut_code: str = "IC"  # IC, IM, or IF
    
    @property
    def index_name(self) -> str:
        mapping = {"IC": "CSI500", "IM": "CSI1000", "IF": "CSI300"}
        return mapping.get(self.fut_code, "Unknown")
    
    @property
    def index_code(self) -> str:
        mapping = {"IC": "000905.SH", "IM": "000852.SH", "IF": "000300.SH"}
        return mapping.get(self.fut_code, "Unknown")


@dataclass
class AccountConfig:
    """Account configuration."""
    initial_capital: float = 10_000_000.0
    default_margin_rate: float = 0.12
    commission_rate: float = 0.00023  # 0.023%
    use_dynamic_margin: bool = True  # Use historical margin rates from data


@dataclass
class StrategyConfig:
    """Strategy configuration."""
    strategy_type: str = "baseline"  # "baseline" or "basis_timing"
    strategy_name: str = "Baseline Roll Strategy"
    
    # Roll parameters
    roll_days_before_expiry: int = 2
    contract_selection: str = "nearby"  # nearby, volume, oi
    min_roll_days: int = 5
    target_leverage: float = 1.0
    roll_criteria: str = "volume"  # volume, oi (for smart_roll strategy)
    liquidity_threshold: float = 0.05  # 5% threshold to avoid ping-pong rolling
    
    # Basis timing parameters (only for basis_timing strategy)
    basis_entry_threshold: float = -0.02  # -2%
    basis_exit_threshold: float = 0.005   # +0.5%
    lookback_window: int = 60
    use_percentile: bool = False
    entry_percentile: float = 0.2
    exit_percentile: float = 0.8
    position_scale_by_basis: bool = False


@dataclass
class BacktestConfig:
    """Backtest configuration."""
    start_date: Optional[str] = None  # YYYY-MM-DD, None for earliest
    end_date: Optional[str] = None    # YYYY-MM-DD, None for latest
    benchmark_name: str = "CSI 500 Index"
    risk_free_rate: float = 0.02  # Annual risk-free rate
    trading_days_per_year: int = TRADING_DAYS_PER_YEAR
    
    # Price field settings (avoid lookahead bias)
    signal_price_field: str = "open"      # Price for signal calculation: open, pre_settle, close
    execution_price_field: str = "open"   # Price for trade execution: open, close


@dataclass
class OutputConfig:
    """Output configuration."""
    output_path: str = str(DEFAULT_OUTPUT_PATH)
    save_plots: bool = True
    save_trade_log: bool = True
    save_nav_series: bool = True
    plot_dpi: int = 150
    figure_format: str = "png"


@dataclass
class Config:
    """Main configuration container."""
    data: DataConfig = field(default_factory=DataConfig)
    account: AccountConfig = field(default_factory=AccountConfig)
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    backtest: BacktestConfig = field(default_factory=BacktestConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    
    @classmethod
    def from_toml(cls, path: str | Path) -> "Config":
        """Load configuration from TOML file."""
        path = Path(path)
        if not path.exists():
            logger.warning(f"Config file not found: {path}, using defaults")
            return cls()
        
        with open(path, "rb") as f:
            raw = tomllib.load(f)
        
        return cls(
            data=DataConfig(**raw.get("data", {})),
            account=AccountConfig(**raw.get("account", {})),
            strategy=StrategyConfig(**raw.get("strategy", {})),
            backtest=BacktestConfig(**raw.get("backtest", {})),
            output=OutputConfig(**raw.get("output", {})),
        )
    
    @classmethod
    def load(cls, path: Optional[str | Path] = None) -> "Config":
        """Load configuration from default or specified path."""
        if path is None:
            path = DEFAULT_CONFIG_PATH
        return cls.from_toml(path)


def load_config(path: Optional[str | Path] = None) -> Config:
    """Convenience function to load configuration."""
    return Config.load(path)
