# Use an official Python runtime as a parent image
FROM python:3.10-slim-bookworm

# Set environment variables to avoid buffering output and prevent bytecode generation
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Define build arguments to pass sensitive data at build time
ARG DB_HOST
ARG DB_PORT
ARG DB_NAME
ARG DB_USER
ARG DB_PASSWORD
ARG SSH_HOST
ARG SSH_USERNAME
ARG SSH_PASSWORD
ARG REMOTE_DIRECTORY
ARG YOUTUBE_API_KEY

# Install system dependencies and psycopg2 for PostgreSQL
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create a directory for the application
WORKDIR /app

# Install Python dependencies directly
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir psycopg2-binary paramiko requests pytz

# Copy the application code to the container
COPY . /app

# Set the environment variables in the container
ENV DB_HOST=$DB_HOST \
    DB_PORT=$DB_PORT \
    DB_NAME=$DB_NAME \
    DB_USER=$DB_USER \
    DB_PASSWORD=$DB_PASSWORD \
    SSH_HOST=$SSH_HOST \
    SSH_USERNAME=$SSH_USERNAME \
    SSH_PASSWORD=$SSH_PASSWORD \
    REMOTE_DIRECTORY=$REMOTE_DIRECTORY \
    YOUTUBE_API_KEY=$YOUTUBE_API_KEY

# Use a non-root user for security purposes (optional)
RUN useradd -ms /bin/bash appuser
USER appuser
