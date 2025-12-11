#!/bin/bash
# Export PlantUML diagrams to PNG
# Usage: ./export_uml.sh
# Requires: plantuml (apt install plantuml) or plantuml.jar

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UML_DIR="$SCRIPT_DIR/uml"
OUTPUT_DIR="$UML_DIR/png"

# Create output directory if not exists
mkdir -p "$OUTPUT_DIR"

echo "========================================"
echo "PlantUML Export Script"
echo "========================================"
echo "Source: $UML_DIR"
echo "Output: $OUTPUT_DIR"
echo ""

# Check if plantuml is available
if command -v plantuml &> /dev/null; then
    PLANTUML_CMD="plantuml"
elif [ -f "$HOME/plantuml.jar" ]; then
    PLANTUML_CMD="java -jar $HOME/plantuml.jar"
elif [ -f "/usr/share/plantuml/plantuml.jar" ]; then
    PLANTUML_CMD="java -jar /usr/share/plantuml/plantuml.jar"
else
    echo "ERROR: PlantUML not found!"
    echo ""
    echo "Install options:"
    echo "  1. apt install plantuml"
    echo "  2. Download plantuml.jar to ~/plantuml.jar"
    echo "     wget https://github.com/plantuml/plantuml/releases/download/v1.2024.7/plantuml-1.2024.7.jar -O ~/plantuml.jar"
    echo ""
    echo "Or use online service:"
    echo "  Visit https://www.plantuml.com/plantuml/uml"
    exit 1
fi

echo "Using: $PLANTUML_CMD"
echo ""

# Export all .puml files
count=0
for puml_file in "$UML_DIR"/*.puml; do
    if [ -f "$puml_file" ]; then
        filename=$(basename "$puml_file" .puml)
        echo "Exporting: $filename.puml -> $filename.png"
        $PLANTUML_CMD -tpng "$puml_file" -o "$OUTPUT_DIR"
        count=$((count + 1))
    fi
done

echo ""
echo "========================================"
echo "Exported $count diagrams to $OUTPUT_DIR"
echo "========================================"

# List generated files
echo ""
echo "Generated files:"
ls -la "$OUTPUT_DIR"/*.png 2>/dev/null || echo "No PNG files generated"
