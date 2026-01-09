#!/bin/bash
# run_full_refresh.sh
# Full dbt refresh orchestration - runs entire pipeline

set -e  # Exit on any error

echo "======================================================================"
echo "🚀 STELLANTIS DBT FULL REFRESH PIPELINE"
echo "======================================================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Start time
START_TIME=$(date +%s)

# Step 1: Clean previous runs
echo -e "${BLUE}Step 1: Cleaning previous dbt artifacts...${NC}"
dbt clean
echo -e "${GREEN}✓ Clean complete${NC}"
echo ""

# Step 2: Install/update dependencies
echo -e "${BLUE}Step 2: Installing dbt packages...${NC}"
dbt deps
echo -e "${GREEN}✓ Dependencies installed${NC}"
echo ""

# Step 3: Debug connection
echo -e "${BLUE}Step 3: Testing database connection...${NC}"
dbt debug --quiet
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Database connection successful${NC}"
else
    echo -e "${RED}✗ Database connection failed${NC}"
    exit 1
fi
echo ""

# Step 4: Run staging models
echo -e "${BLUE}Step 4: Running staging models...${NC}"
dbt run --select staging.*
echo -e "${GREEN}✓ Staging models complete${NC}"
echo ""

# Step 5: Run intermediate models
echo -e "${BLUE}Step 5: Running intermediate models...${NC}"
dbt run --select intermediate.*
echo -e "${GREEN}✓ Intermediate models complete${NC}"
echo ""

# Step 6: Run dimensions
echo -e "${BLUE}Step 6: Running dimension models...${NC}"
dbt run --select dimensions.*
echo -e "${GREEN}✓ Dimension models complete${NC}"
echo ""

# Step 7: Run marts
echo -e "${BLUE}Step 7: Running mart models...${NC}"
dbt run --select marts.*
echo -e "${GREEN}✓ Mart models complete${NC}"
echo ""

# Step 8: Run all tests
echo -e "${BLUE}Step 8: Running data quality tests...${NC}"
dbt test
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed${NC}"
else
    echo -e "${YELLOW}⚠ Some tests failed - check logs${NC}"
fi
echo ""

# Step 9: Get row counts
echo -e "${BLUE}Step 9: Generating row count report...${NC}"
dbt run-operation get_row_counts
echo ""

# Step 10: Generate documentation
echo -e "${BLUE}Step 10: Generating documentation...${NC}"
dbt docs generate
echo -e "${GREEN}✓ Documentation generated${NC}"
echo ""

# End time and duration
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo "======================================================================"
echo -e "${GREEN}🎉 FULL REFRESH COMPLETED SUCCESSFULLY!${NC}"
echo "======================================================================"
echo "Total duration: ${DURATION} seconds"
echo "Documentation available at: target/index.html"
echo ""
echo "To view docs, run: dbt docs serve"
echo "======================================================================"
