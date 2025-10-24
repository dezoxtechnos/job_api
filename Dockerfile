# ========================
# Dockerfile for Render
# ========================
FROM python:3.11-slim

WORKDIR /app

# Prevents Python from writing pyc files & buffering stdout
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install dependencies needed for psycopg2, lxml, etc.
RUN apt-get update && apt-get install -y build-essential libpq-dev gcc libxml2-dev libxslt1-dev && rm -rf /var/lib/apt/lists/*

# Copy all files
COPY . .

# Install Python packages
RUN pip install --upgrade pip && pip install -r requirements.txt

# Expose Flask port (Render uses dynamic $PORT)
EXPOSE 10000

# Run Flask API via Gunicorn
CMD ["gunicorn", "app:app", "-w", "4", "-b", "0.0.0.0:10000"]