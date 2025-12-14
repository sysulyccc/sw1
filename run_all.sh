#!/bin/bash
# Run all strategies sequentially
# Usage: ./run_all.sh

echo "Running all strategies..."
echo "========================="

for config in configs/strategies/*.toml; do
    strategy=$(basename "$config" .toml)
    echo ""
    echo ">>> Running: $strategy"
    echo "-------------------------------------------"
    python main.py "$config"
done

echo ""
echo "========================="
echo "All strategies completed!"
