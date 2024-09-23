# Use an official Python runtime as a parent image
FROM python:3.10-slim-bookworm

# Set environment variables to avoid buffering output and prevent bytecode generation
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install necessary packages and update GPG keys
RUN apt-get update && apt-get install -y --no-install-recommends \
    gnupg2 \
    ca-certificates \
    && apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 648ACFD622F3D138 112695A0E562B32A \
    && apt-get update && apt-get install -y --no-install-recommends \
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

# Use a non-root user for security purposes (optional)
RUN useradd -ms /bin/bash appuser
USER appuser

# Command to run the Python script
CMD ["python", "schrif_video_weg.py"]
