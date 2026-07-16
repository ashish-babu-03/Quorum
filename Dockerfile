FROM gradle:8.9-jdk17 AS build
WORKDIR /app
COPY . .
RUN gradle installDist --no-daemon

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/build/install/raft-lock-manager /app
ENTRYPOINT ["/app/bin/raft-lock-manager"]
