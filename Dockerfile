# Stage 1: Build the application using Maven
FROM maven:3.9.6-eclipse-temurin-21 AS builder

WORKDIR /app

# Copy pom.xml first for better Docker layer caching
COPY pom.xml .
COPY mvnw .
COPY mvnw.cmd .
COPY .mvn .mvn

# Download dependencies
RUN mvn dependency:go-offline -B

# Copy source code
COPY src ./src

# Build the JAR
RUN mvn clean package -DskipTests

# Stage 2: Run the built Spring Boot JAR
FROM openjdk:21-slim

WORKDIR /app

# Copy the generated JAR from the builder stage
COPY --from=builder /app/target/*.jar app.jar

# Render uses PORT environment variable (dynamic)
EXPOSE $PORT

# Render requires listening on 0.0.0.0 and dynamic PORT
# ["java", "-Dserver.port=$PORT", "-jar", "app.jar"]

ENTRYPOINT ["java", "-jar", "app.jar"]