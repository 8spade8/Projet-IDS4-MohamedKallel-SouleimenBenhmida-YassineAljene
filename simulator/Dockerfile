# Dockerfile — Smart Meter Simulator
# Lightweight Python image that runs the simulator inside the Docker network.

FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir kafka-python==2.0.2

# Copy simulator script
COPY smart_meter_simulator.py .

# The KAFKA_BROKER env var is injected by docker-compose
ENV KAFKA_BROKER=kafka:9092

CMD ["python", "smart_meter_simulator.py"]
