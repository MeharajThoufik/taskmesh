# Lightweight runtime image: copies the pre-built Spring Boot jar into a JRE 17 base.
# Build the jar first with `./mvnw clean package`, then `docker compose up -d --build`.
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY target/producer-service-*.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
