#!/bin/bash
# ================================================================
# Stellantis Manufacturing Analytics - Database Setup
# Description: Complete database initialization from scratch
# ================================================================

echo "🚀 Starting database setup..."

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Database name
DB_NAME="stellantis_manufacturing"

# Step 1: Drop database if exists (careful!)
echo "📦 Step 1: Dropping existing database (if any)..."
psql -U postgres -c "DROP DATABASE IF EXISTS $DB_NAME;" 2>/dev/null

# Step 2: Create fresh database
echo "📦 Step 2: Creating database..."
psql -U postgres -c "CREATE DATABASE $DB_NAME;"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Database created successfully${NC}"
else
    echo -e "${RED}❌ Failed to create database${NC}"
    exit 1
fi

# Step 3: Run DDL scripts
echo "📦 Step 3: Creating schemas..."
psql -U postgres -d $DB_NAME -f ddl/01_create_schema.sql

echo "📦 Step 4: Creating dimension tables..."
psql -U postgres -d $DB_NAME -f ddl/02_create_dimensions.sql

echo "📦 Step 5: Creating fact table..."
psql -U postgres -d $DB_NAME -f ddl/03_create_fact_table.sql

echo "📦 Step 6: Creating indexes..."
psql -U postgres -d $DB_NAME -f ddl/04_create_indexes.sql

# Step 4: Verify
echo "📦 Step 7: Verifying setup..."
TABLE_COUNT=$(psql -U postgres -d $DB_NAME -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'warehouse';")

if [ "$TABLE_COUNT" -eq 7 ]; then
    echo -e "${GREEN}✅ Setup complete! 7 tables created.${NC}"
else
    echo -e "${RED}❌ Setup incomplete. Expected 7 tables, found $TABLE_COUNT${NC}"
    exit 1
fi

echo ""
echo "🎉 Database ready!"
echo "   Connect: psql -U postgres -d $DB_NAME"
