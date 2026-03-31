FROM apache/hive:4.0.0

USER root
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
RUN curl -L -o /opt/hive/lib/postgresql-42.6.0.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar

USER hive
