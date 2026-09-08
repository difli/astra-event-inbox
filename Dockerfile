# =============================================================================
# Multi-stage build for astra-event-inbox
# Stage 1 – build with Maven (maven:3.9-eclipse-temurin-21 includes both Maven and JDK)
# Stage 2 – minimal JRE runtime image
# =============================================================================

# ---- Build stage ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace

# Copy pom.xml and download dependencies before copying sources
# (improves layer caching when only source code changes)
COPY pom.xml .
RUN mvn dependency:go-offline -B --no-transfer-progress

COPY src ./src
RUN mvn package -DskipTests -B --no-transfer-progress

# ---- Runtime stage ----
FROM eclipse-temurin:21-jre-jammy AS runtime

# Non-root user
RUN groupadd -r inbox && useradd -r -g inbox inbox
WORKDIR /app

# Copy the fat jar
COPY --from=build /workspace/target/astra-event-inbox-*.jar app.jar

# The Astra Secure Connect Bundle is mounted at runtime;
# its path must match ASTRA_SECURE_BUNDLE_PATH.
VOLUME ["/app/certs"]

# Health check via Spring Boot Actuator using wget (present in base image, no extra install needed)
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD wget -qO- http://localhost:8080/actuator/health || exit 1

USER inbox
EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
