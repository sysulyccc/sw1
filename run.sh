#!/bin/bash
# Quick backtest runner
# Usage:
#   ./run.sh <strategy_name>
#
# Examples:
#   ./run.sh baseline
#   ./run.sh fixed_lot_baseline
#   ./run.sh basis_timing_roll

list_strategies() {
    for f in configs/strategies/*.toml; do
        basename "$f" .toml
    done
}

if [ -z "$1" ]; then
    echo "Usage: ./run.sh <strategy_name>"
    echo ""
    echo "Available strategies:"
    list_strategies
    exit 1
fi

STRATEGY=$1
CONFIG_PATH="configs/strategies/${STRATEGY}.toml"

if [ ! -f "$CONFIG_PATH" ]; then
    echo "Error: Strategy config '$STRATEGY' not found."
    echo ""
    echo "Available strategies:"
    list_strategies
    exit 1
fi

python main.py "$CONFIG_PATH"
