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

# Install Python dependencies, including python-dotenv, scikit-learn, joblib, and pandas
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir pandas scikit-learn joblib psycopg2-binary paramiko requests pytz python-dotenv youtube-transcript-api

# Copy the application code to the container
COPY . /app

# Use a non-root user for security purposes (optional)
RUN useradd -ms /bin/bash appuser
USER appuser
CMD ["python", "schrif_video_weg.py"]
