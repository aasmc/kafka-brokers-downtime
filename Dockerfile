FROM maven:3.9.11-eclipse-temurin-25 AS build
WORKDIR /workspace
COPY pom.xml .
RUN mvn -q -DskipTests dependency:go-offline
COPY src src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /workspace/target/kafka-downtime-experiment-1.0.0.jar app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
