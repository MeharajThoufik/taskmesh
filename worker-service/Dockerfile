# Lightweight runtime image: copies the pre-built Spring Boot jar into a JRE 17 base.
# Build the jar first with `./mvnw clean package`, then `docker compose up -d --build`.
# WORKER_ID, POSTGRES_URL, REDIS_HOST, KAFKA_BOOTSTRAP_SERVERS are supplied via env.
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY target/worker-service-*.jar app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
