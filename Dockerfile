
FROM eclipse-temurin:17-jre

WORKDIR /app

COPY target/ms-campuslab-audi-0.0.1-SNAPSHOT.jar app.jar

EXPOSE 8084

ENTRYPOINT ["java", "-jar", "app.jar"]
