param(
    [ValidateSet('start', 'stop', 'clean', 'logs', 'status', 'tables', 'hdfs-ls', 'spark-logs')]
    [string]$Action = 'start'
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $MyInvocation.MyCommand.Path

switch ($Action) {
    'start' {
        docker compose up -d --build
        Write-Host ''
        Write-Host 'Services:'
        Write-Host '  HDFS NameNode UI : http://localhost:9870'
        Write-Host '  Spark Master UI  : http://localhost:18080'
        Write-Host '  Live Dashboard   : http://localhost:8501'
        Write-Host '  HiveServer2      : localhost:10000'
        Write-Host '  Kafka (host)     : localhost:29092'
    }
    'stop' {
        docker compose stop
    }
    'clean' {
        docker compose down -v
    }
    'logs' {
        docker compose logs -f
    }
    'status' {
        docker compose ps -a
    }
    'tables' {
        docker exec hive-server /opt/hive/bin/beeline -u 'jdbc:hive2://localhost:10000' -n root -f /hive_scripts/create_tables.hql
    }
    'hdfs-ls' {
        docker exec namenode hdfs dfs -ls -R /data/energy
    }
    'spark-logs' {
        docker compose logs -f spark-job
    }
}
