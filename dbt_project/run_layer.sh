#!/bin/bash
# Run specific layer
# Usage: ./run_layer.sh staging|intermediate|marts|dimensions

LAYER=$1

if [ -z "$LAYER" ]; then
    echo "Usage: ./run_layer.sh [staging|intermediate|marts|dimensions]"
    exit 1
fi

echo "🎯 Running $LAYER layer..."
dbt run --select $LAYER.*
dbt test --select $LAYER.*
echo "✅ $LAYER complete!"
