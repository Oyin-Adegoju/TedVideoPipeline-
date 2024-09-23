# Stage 1: Use a Debian-based image with the required dependencies
FROM debian:bullseye-slim as builder

# Set environment variables to avoid buffering output and prevent bytecode generation
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies and psycopg2 for PostgreSQL
RUN apt-get update --allow-releaseinfo-change --allow-unauthenticated && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Stage 2: Use Python runtime with pre-installed psycopg2
FROM python:3.10-slim-bookworm

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Copy installed packages from the builder stage
COPY --from=builder /usr/lib /usr/lib
COPY --from=builder /usr/local /usr/local

# Create a directory for the application
WORKDIR /app

# Install Python dependencies directly (assuming you still need pip dependencies)
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir paramiko requests pytz

# Copy the application code to the container
COPY . /app

# Use a non-root user for security purposes (optional)
RUN useradd -ms /bin/bash appuser
USER appuser
