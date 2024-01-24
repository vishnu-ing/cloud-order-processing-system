# syntax=docker/dockerfile:1
# Build context is the repository root: docker build -f order-service/Dockerfile .
# ---------------------------------------------------------------------------
# Stage 1 - build only this module and its dependency, with a cached dependency layer
# ---------------------------------------------------------------------------
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /workspace

# Copy the reactor poms first so `go-offline` is cached until a dependency actually changes.
COPY pom.xml ./
# Every module pom is needed even though only two modules are built: Maven reads the root
# pom's <modules> list and refuses to start if a declared module has no pom.
COPY common-events/pom.xml common-events/
COPY order-service/pom.xml order-service/
COPY inventory-service/pom.xml inventory-service/
COPY payment-service/pom.xml payment-service/
COPY notification-service/pom.xml notification-service/
COPY api-gateway/pom.xml api-gateway/
COPY lambda-functions/pom.xml lambda-functions/
RUN mvn -B -ntp -pl common-events,order-service -am dependency:go-offline

COPY common-events/src common-events/src
COPY order-service/src order-service/src
RUN mvn -B -ntp -pl common-events,order-service -am -DskipTests package

# ---------------------------------------------------------------------------
# Stage 2 - explode the layered jar so Docker can cache dependencies separately from code
# ---------------------------------------------------------------------------
FROM eclipse-temurin:21-jre-jammy AS layers
WORKDIR /layers
COPY --from=build /workspace/order-service/target/order-service.jar application.jar
RUN java -Djarmode=layertools -jar application.jar extract

# ---------------------------------------------------------------------------
# Stage 3 - minimal runtime, non-root
# ---------------------------------------------------------------------------
FROM eclipse-temurin:21-jre-jammy
ENV SERVER_PORT=8081 \
    JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError"

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 spring \
    && useradd --system --uid 10001 --gid spring --home /app spring

WORKDIR /app
COPY --from=layers --chown=spring:spring /layers/dependencies/ ./
COPY --from=layers --chown=spring:spring /layers/spring-boot-loader/ ./
COPY --from=layers --chown=spring:spring /layers/snapshot-dependencies/ ./
COPY --from=layers --chown=spring:spring /layers/application/ ./

USER spring:spring
EXPOSE 8081

HEALTHCHECK --interval=15s --timeout=3s --start-period=45s --retries=4 \
  CMD curl -fsS http://localhost:${SERVER_PORT}/actuator/health/readiness || exit 1

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS org.springframework.boot.loader.launch.JarLauncher"]
