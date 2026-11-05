# Multi-stage build: copy GOWA binary from official Alpine image, run alongside Python
FROM ghcr.io/aldinokemal/go-whatsapp-web-multidevice:v9.8.0 AS gowa

FROM python:3.12-alpine

# Install runtime deps (Alpine packages)
RUN apk add --no-cache \
    ca-certificates \
    curl \
    ffmpeg \
    libwebp \
    tzdata

# Copy GOWA binary from official image (storages dir created at runtime)
COPY --from=gowa /app/whatsapp /app/whatsapp

# Set up app with persistent data directory
WORKDIR /app
RUN mkdir -p /app/data /app/storages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY templates/ templates/

# Startup script
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

ENV PORT=8000
ENV GOWA_URL=http://127.0.0.1:3000
# APP_PASSWORD and GOWA_BASIC_AUTH must be set at runtime (no defaults)
ENV DATABASE_URL=sqlite:////app/data/miner.db

# FIXED: only expose FastAPI port, not GOWA
EXPOSE 8000

CMD ["/app/start.sh"]
