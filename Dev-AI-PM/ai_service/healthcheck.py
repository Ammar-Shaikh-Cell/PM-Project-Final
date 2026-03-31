#!/usr/bin/env python3
"""Simple healthcheck script for AI service"""
import sys
import http.client

try:
    conn = http.client.HTTPConnection("localhost", 8000, timeout=5)
    conn.request("GET", "/health")
    response = conn.getresponse()
    
    if response.status == 200:
        sys.exit(0)  # Healthy
    else:
        sys.exit(1)  # Unhealthy
except Exception:
    sys.exit(1)  # Unhealthy
