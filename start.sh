#!/usr/bin/env bash
# =============================================================================
# start.sh — One-command launcher for the energy fraud detection stack
# =============================================================================
# Usage:
#   bash start.sh          → start all services
#   bash start.sh stop     → stop all services (keep volumes)
#   bash start.sh clean    → stop and delete all volumes
#   bash start.sh logs     → follow all container logs
#   bash start.sh status   → show running containers
#   bash start.sh hive     → open a beeline shell on HiveServer2
#   bash start.sh kafka    → open a Kafka console consumer
# =============================================================================

set -e

CMD=${1:-start}

case "$CMD" in

  start)
    echo "Starting full stack..."
    docker compose up -d --build
    echo ""
    echo "=== Services starting up ==="
    echo "  Kafka UI (optional) : http://localhost:9092"
    echo "  HDFS NameNode UI    : http://localhost:9870"
    echo "  Spark Master UI     : http://localhost:8080"
    echo "  Hive Web UI         : http://localhost:10002"
    echo ""
    echo "Waiting for simulator to start..."
    sleep 5
    docker compose logs --tail=20 simulator
    ;;

  stop)
    echo "Stopping all services..."
    docker compose stop
    ;;

  clean)
    echo "Removing all containers and volumes..."
    docker compose down -v
    ;;

  logs)
    docker compose logs -f
    ;;

  status)
    docker compose ps
    ;;

  hive)
    echo "Opening beeline shell on HiveServer2..."
    docker exec -it hive-server \
      /opt/hive/bin/beeline -u "jdbc:hive2://localhost:10000" -n root
    ;;

  kafka)
    echo "Opening Kafka console consumer on energy_readings..."
    docker exec -it kafka \
      kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic energy_readings \
        --from-beginning
    ;;

  hdfs-ls)
    echo "Listing HDFS /data/energy..."
    docker exec -it namenode \
      hdfs dfs -ls -R /data/energy
    ;;

  spark-logs)
    echo "Following Spark job logs..."
    docker compose logs -f spark-job
    ;;

  *)
    echo "Unknown command: $CMD"
    echo "Usage: bash start.sh [start|stop|clean|logs|status|hive|kafka|hdfs-ls|spark-logs]"
    exit 1
    ;;
esac
