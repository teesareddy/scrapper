# Use Python 3.11 slim as base image
FROM python:3.11-slim-bullseye


# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DJANGO_SETTINGS_MODULE=config.settings \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    # Set a consistent path for Playwright browsers
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Create app directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    python3-dev \
    postgresql-client \
    netcat-traditional \
    curl \
    # Playwright dependencies
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libgtk-3-0 \
    fonts-liberation \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright and browsers globally as root
RUN pip install --no-cache-dir playwright && \
    mkdir -p /ms-playwright && \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright playwright install chromium && \
    chmod -R 777 /ms-playwright


# Create django user and directories
RUN groupadd -r django && \
    useradd -r -g django -d /home/django -m django

# Copy project code and set permissions
COPY --chown=django:django . .

# Copy and set up entrypoint
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# Switch to django user
USER django

# Expose port
EXPOSE 8000

# Use the entrypoint script
ENTRYPOINT ["/docker-entrypoint.sh"]

# Default command
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]