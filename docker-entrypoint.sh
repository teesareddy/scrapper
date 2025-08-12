#!/bin/sh
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}Django Application Startup Script${NC}"
echo "============================================"

# Function to wait for a service
wait_for_service() {
    local host="$1"
    local port="$2"
    local service="$3"
    local timeout=60
    local attempt=0

    echo -e "${YELLOW}Waiting for $service at $host:$port...${NC}"
    until nc -z "$host" "$port" || [ $attempt -eq $timeout ]; do
        echo -n "."
        sleep 1
        attempt=$((attempt+1))
    done

    if [ $attempt -eq $timeout ]; then
        echo -e "${RED}$service failed to start within $timeout seconds${NC}"
        exit 1
    fi
    echo -e "${GREEN}$service is ready!${NC}"
}

# Wait for required services
if [ "$DATABASE" = "postgres" ]; then
    wait_for_service "$SQL_HOST" "$SQL_PORT" "PostgreSQL"
fi

if [ -n "$RABBITMQ_HOST" ]; then
    wait_for_service "$RABBITMQ_HOST" "$RABBITMQ_PORT" "RabbitMQ"
fi

if [ -n "$CELERY_BROKER_URL" ]; then
    REDIS_HOST=$(echo "$CELERY_BROKER_URL" | awk -F[/:] '{print $4}')
    REDIS_PORT=$(echo "$CELERY_BROKER_URL" | awk -F[/:] '{print $5}')
    if [ -n "$REDIS_HOST" ] && [ -n "$REDIS_PORT" ]; then
        wait_for_service "$REDIS_HOST" "$REDIS_PORT" "Redis"
    fi
fi

# Verify Playwright installation
echo -e "${YELLOW}Verifying Playwright installation...${NC}"
if [ -d "$PLAYWRIGHT_BROWSERS_PATH" ]; then
    echo -e "${GREEN}✓ Playwright browsers directory exists at $PLAYWRIGHT_BROWSERS_PATH${NC}"
    ls -la "$PLAYWRIGHT_BROWSERS_PATH" || true

    # Check for Chromium executable specifically
    CHROMIUM_PATH=$(find "$PLAYWRIGHT_BROWSERS_PATH" -name "chrome" -o -name "chrome.exe" | head -1)
    if [ -n "$CHROMIUM_PATH" ]; then
        echo -e "${GREEN}✓ Chromium executable found at $CHROMIUM_PATH${NC}"
        if [ -x "$CHROMIUM_PATH" ]; then
            echo -e "${GREEN}✓ Chromium executable has execute permissions${NC}"
        else
            echo -e "${YELLOW}⚠ Setting execute permission on Chromium executable${NC}"
            chmod +x "$CHROMIUM_PATH"
        fi
    else
        echo -e "${RED}✗ No Chromium executable found in $PLAYWRIGHT_BROWSERS_PATH${NC}"
        echo -e "${YELLOW}Attempting to install Playwright browsers...${NC}"
        playwright install chromium
    fi
else
    echo -e "${RED}✗ Playwright browsers directory not found at $PLAYWRIGHT_BROWSERS_PATH${NC}"
    echo -e "${YELLOW}Creating directory and installing Playwright browsers...${NC}"
    mkdir -p "$PLAYWRIGHT_BROWSERS_PATH"
    chmod -R 777 "$PLAYWRIGHT_BROWSERS_PATH"
    playwright install chromium
fi

# Check if running as Django service
IS_MAIN_DJANGO=0
if [ "$1" = "python" ] && echo "$@" | grep -q "runserver\|consume"; then
    IS_MAIN_DJANGO=1
fi

# Database setup (only for Django service)
if [ $IS_MAIN_DJANGO -eq 1 ]; then
    echo -e "${YELLOW}Setting up database...${NC}"
    if python manage.py migrate --no-input; then
        echo -e "${GREEN}✓ Database migrations completed${NC}"
    else
        echo -e "${YELLOW}⚠ Could not run migrations${NC}"
    fi

    if [ "$DEBUG" = "1" ] && [ -n "$DJANGO_SUPERUSER_USERNAME" ]; then
        echo -e "${YELLOW}Creating superuser...${NC}"
        python manage.py createsuperuser \
            --username "$DJANGO_SUPERUSER_USERNAME" \
            --email "${DJANGO_SUPERUSER_EMAIL:-admin@example.com}" \
            --noinput 2>/dev/null || echo -e "${YELLOW}⚠ Superuser already exists${NC}"
    fi

    # Collect static files if needed
    echo -e "${YELLOW}Collecting static files...${NC}"
    python manage.py collectstatic --no-input --clear || echo -e "${YELLOW}⚠ Static files collection skipped${NC}"
fi

echo -e "${GREEN}Starting service...${NC}"
echo "============================================"

# Execute the command
exec "$@"