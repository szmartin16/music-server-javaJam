FROM eclipse-temurin:21-jdk
WORKDIR /app
COPY src ./src
RUN mkdir -p out && javac -d out src/main/java/MatchmakingServer.java
EXPOSE 8080
CMD ["java", "-cp", "out", "main.java.MatchmakingServer"]
