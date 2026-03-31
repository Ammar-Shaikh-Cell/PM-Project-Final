FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

# FreeTDS for pymssql (supports SQL Server 2000 / 2001 and newer)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      freetds-dev freetds-common gcc && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY poller.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "poller.py"]
