# Python image
FROM python:3.12-slim

# Working directory
WORKDIR /app

# Installing system dpendencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy application files
COPY . /app

# Installing dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port
EXPOSE 8080

# Command to run the app using Gunicorn
CMD ["gunicorn", "-b", "0.0.0.0:8080", "--workers", "1", "--threads", "8", "--timeout", "0", "app:app"]