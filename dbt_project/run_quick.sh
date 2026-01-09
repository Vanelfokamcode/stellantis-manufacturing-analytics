#!/bin/bash
# Quick run - models only, skip tests

echo "🚀 Quick dbt run (no tests)..."
dbt run
echo "✅ Complete! Duration: $SECONDS seconds"

