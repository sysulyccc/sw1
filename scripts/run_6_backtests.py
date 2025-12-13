from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from pathlib import Path
import sys

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_config
from main import run_backtest_from_config


def _set_local_paths(cfg):
    cfg.data.processed_data_path = str(PROJECT_ROOT / "processed_data")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    cfg.output.output_path = str(PROJECT_ROOT / "output" / f"compare_6_strategies_{ts}")
    return cfg


def main():
    base_cfg = load_config()
    base_cfg = _set_local_paths(base_cfg)

    runs = [
        ("baseline", "notional"),
        ("smart_roll", "notional"),
        ("basis_timing", "notional"),
        ("baseline_fixed_lot", "fixed_lot"),
        ("smart_roll_fixed_lot", "fixed_lot"),
        ("basis_timing_fixed_lot", "fixed_lot"),
    ]

    results = []
    for strategy_type, position_mode in runs:
        cfg = deepcopy(base_cfg)
        cfg.strategy.strategy_type = strategy_type
        cfg.strategy.position_mode = position_mode

        logger.info(f"Running: strategy_type={strategy_type}, position_mode={position_mode}")
        result = run_backtest_from_config(cfg)
        metrics = result.metrics

        results.append(
            {
                "strategy_type": strategy_type,
                "position_mode": position_mode,
                "annualized_return": metrics.get("annualized_return"),
                "annualized_volatility": metrics.get("annualized_volatility"),
                "sharpe_ratio": metrics.get("sharpe_ratio"),
                "max_drawdown": metrics.get("max_drawdown"),
                "benchmark_return": metrics.get("benchmark_return"),
                "alpha": metrics.get("alpha"),
            }
        )

    print("\n=== Summary (6 strategies) ===")
    header = [
        "strategy_type",
        "position_mode",
        "annualized_return",
        "annualized_volatility",
        "sharpe_ratio",
        "max_drawdown",
        "benchmark_return",
        "alpha",
    ]
    print("\t".join(header))
    for r in results:
        row = []
        for k in header:
            v = r.get(k)
            if isinstance(v, float):
                row.append(f"{v:.6f}")
            else:
                row.append(str(v))
        print("\t".join(row))


if __name__ == "__main__":
    main()
