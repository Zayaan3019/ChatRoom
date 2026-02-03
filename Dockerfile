# Multi-stage Dockerfile for Production Deployment
# Optimized for minimal image size and security

# Stage 1: Builder
FROM python:3.11-slim as builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --user -r requirements.txt


# Stage 2: Runtime
FROM python:3.11-slim

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash chatuser

WORKDIR /app

# Copy Python packages from builder
COPY --from=builder /root/.local /home/chatuser/.local

# Copy application code
COPY app/ ./app/

# Change ownership
RUN chown -R chatuser:chatuser /app

# Switch to non-root user
USER chatuser

# Add local bin to PATH
ENV PATH=/home/chatuser/.local/bin:$PATH

# Expose server port
EXPOSE 8888

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import socket; s=socket.socket(); s.connect(('127.0.0.1', 8888)); s.close()" || exit 1

# Default command: run server
CMD ["python", "-m", "app.main", "server", "--host", "0.0.0.0", "--port", "8888"]
