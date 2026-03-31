#!/bin/bash
# Bash script to create/update .env file with MSSQL configuration
# Usage: ./setup_env.sh <MSSQL_USERNAME> <MSSQL_PASSWORD> [MSSQL_HOST] [MSSQL_PORT]

MSSQL_USERNAME="${1}"
MSSQL_PASSWORD="${2}"
MSSQL_HOST="${3:-10.1.61.252}"
MSSQL_PORT="${4:-1433}"
MSSQL_DATABASE="${5:-HISTORISCH}"
MSSQL_TABLE="${6:-Tab_Actual}"
MSSQL_SCHEMA="${7:-dbo}"

if [ -z "$MSSQL_USERNAME" ] || [ -z "$MSSQL_PASSWORD" ]; then
    echo "Usage: $0 <MSSQL_USERNAME> <MSSQL_PASSWORD> [MSSQL_HOST] [MSSQL_PORT] [MSSQL_DATABASE] [MSSQL_TABLE] [MSSQL_SCHEMA]"
    echo ""
    echo "Example:"
    echo "  $0 'myuser' 'mypassword' '10.1.61.252' '1433'"
    exit 1
fi

echo "Setting up .env file with MSSQL configuration..."
echo ""

# Check if .env exists
if [ -f ".env" ]; then
    echo "⚠️  .env file already exists. Backing up to .env.backup..."
    cp .env .env.backup
    echo "✅ Backup created: .env.backup"
    echo ""
fi

# MSSQL Configuration
MSSQL_CONFIG="# ============================================
# MSSQL Extruder Poller Configuration
# ============================================
# Enable/disable MSSQL poller (master switch)
MSSQL_ENABLED=true

# MSSQL Server Connection
MSSQL_HOST=$MSSQL_HOST
MSSQL_PORT=$MSSQL_PORT
MSSQL_USER=$MSSQL_USERNAME
MSSQL_PASSWORD=$MSSQL_PASSWORD

# MSSQL Database and Table
MSSQL_DATABASE=$MSSQL_DATABASE
MSSQL_TABLE=$MSSQL_TABLE
MSSQL_SCHEMA=$MSSQL_SCHEMA

# Optional: Advanced Poller Settings
MSSQL_POLL_INTERVAL_SECONDS=60
MSSQL_WINDOW_MINUTES=10
MSSQL_MAX_ROWS_PER_POLL=5000
MSSQL_MACHINE_NAME=Extruder-SQL
MSSQL_SENSOR_NAME=Extruder SQL Snapshot"

if [ -f ".env" ]; then
    # Check if MSSQL config already exists
    if grep -q "MSSQL_ENABLED" .env; then
        echo "⚠️  MSSQL configuration already exists in .env"
        echo "Updating existing MSSQL configuration..."
        
        # Remove old MSSQL lines (simple approach - remove lines starting with MSSQL_ or # MSSQL)
        sed -i.bak '/^#.*MSSQL/d; /^MSSQL_/d' .env
        rm -f .env.bak
        
        # Append new config
        echo "" >> .env
        echo "$MSSQL_CONFIG" >> .env
    else
        # Append MSSQL config
        echo "" >> .env
        echo "$MSSQL_CONFIG" >> .env
    fi
else
    # Create new .env file with basic config
    cat > .env << EOF
# PostgreSQL Database Configuration
POSTGRES_USER=pm_user
POSTGRES_PASSWORD=pm_pass
POSTGRES_DB=pm_db

# JWT Configuration
JWT_SECRET=change-me-in-production
JWT_ALGORITHM=HS256
JWT_EXP_MINUTES=60

$MSSQL_CONFIG
EOF
fi

echo ""
echo "✅ .env file updated successfully!"
echo ""
echo "MSSQL Configuration:"
echo "  Host: $MSSQL_HOST"
echo "  Port: $MSSQL_PORT"
echo "  Database: $MSSQL_DATABASE"
echo "  Table: $MSSQL_TABLE"
echo "  Username: $MSSQL_USERNAME"
echo "  Password: ********"
echo ""
echo "⚠️  Next Steps:"
echo "  1. Restart Docker containers: docker-compose restart backend"
echo "  2. Or rebuild: docker-compose up -d --build backend"
echo "  3. Check status: curl http://localhost:8000/dashboard/extruder/status"
