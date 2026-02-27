FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml ./
COPY src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/target/telegram-shopify-bot.jar /app/app.jar
ENV SQLITE_PATH=/data/bot.db
VOLUME ["/data"]
CMD ["java", "-jar", "/app/app.jar"]
