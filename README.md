# Index Enhancement Strategy Backtest System

## Quick Start

```bash
# Run backtest with a strategy
bash run.sh baseline              # Baseline strategy
bash run.sh fixed_lot_baseline    # Fixed lot version
bash run.sh basis_timing_roll     # Basis timing roll

# List all available strategies
bash run.sh

# Run all strategies
bash run_all.sh
```

**Available strategies:**
- `baseline`, `smart_roll`, `basis_timing`, `basis_timing_roll`, `spread_timing`, `liquidity_roll`, `aery_roll`
- Fixed lot versions: `fixed_lot_baseline`, `fixed_lot_smart_roll`, `fixed_lot_basis_timing`, `fixed_lot_basis_timing_roll`, `fixed_lot_spread_timing`, `fixed_lot_liquidity_roll`, `fixed_lot_aery_roll`

Strategy configs are in `configs/strategies/`. Edit them to customize parameters (e.g., `fut_code`, `start_date`).

---

## Project Overview

This is an OOP-based backtest system for equity index futures enhancement strategies. It captures excess returns over the benchmark by leveraging the tendency of a discounted futures price to converge to the spot index level as the contract approaches expiration.

## Quick Start

### 1. Preprocess Data

```bash
python scripts/filesync_client.py # get data from filesync
python scripts/preprocess_data_from_wind.py
```

### 2. Configure Strategy

Edit `config.toml` to choose:
- **fut_code**: `IC` (CSI500), `IM` (CSI1000), or `IF` (CSI300)
- **strategy_type**: `baseline` or `basis_timing`
- **parameters**: roll days, leverage, thresholds, etc.

### 3. Run Backtest

```bash
python main.py                    # Use default config.toml
python main.py path/to/config.toml  # Use custom config
```

### 4. View Results

Results are saved to `output/{strategy}_{fut}/`:
- `report.png` - Visual dashboard with NAV, drawdown, metrics
- `trade_log.csv` - All trades with timestamps and prices
- `nav_series.csv` - Daily NAV for further analysis

### 5. Run Tests

```bash
pytest tests/ -v
```

## Dependencies

- Python 3.11+
- polars, pandas, numpy
- matplotlib
- loguru
- pytest (for testing)

## Documentation

- [Design Document](docs/DESIGN.md) - System architecture and class design
- [Backtest Flow](docs/BACKTEST_FLOW.md) - Step-by-step simulation process
- [Examples](examples/) - Interactive Jupyter notebooks

## License

Academic use only.
