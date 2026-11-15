# syntax=docker/dockerfile:1
# ---------- Build: compile the Spring Boot jar ----------
# Tests are not run here: they need Docker (Testcontainers). CI runs them as a separate step.
FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn -B -q dependency:go-offline
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q package -DskipTests \
 && cp target/consumer-service-*.jar /src/app.jar

# ---------- Run: small JRE image, non-root user ----------
FROM eclipse-temurin:21-jre
RUN groupadd --system app && useradd --system --gid app app
# The service runs from /app/service, so the .env-style relative key path
# ../keys/snowflake_rsa_key.p8 resolves to /app/keys (mounted read-only by compose).
WORKDIR /app/service
COPY --from=build /src/app.jar app.jar
USER app
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=40s --retries=6 \
  CMD curl -fs http://localhost:8080/actuator/health || exit 1
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "app.jar"]
