FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app:/app/live_monitor

WORKDIR /app

COPY live_monitor/requirements.txt /app/live_monitor/requirements.txt
RUN pip install --no-cache-dir -r /app/live_monitor/requirements.txt

COPY . /app

CMD ["python", "live_monitor/main.py"]
