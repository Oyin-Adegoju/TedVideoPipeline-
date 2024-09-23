# Use an official Python runtime as a parent image
FROM python:3.10-slim-bookworm

# Set environment variables to avoid buffering output and prevent bytecode generation
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

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

# Command to run your Python script
CMD ["python", "schrif_video_weg.py"]
