# Native image. Build:  docker build -t magix:native .
FROM ghcr.io/graalvm/native-image-community:21 AS build
WORKDIR /src
COPY .mvn .mvn
COPY mvnw pom.xml ./
COPY src src
RUN ./mvnw -B -Dpackaging=native-image -DskipTests package

FROM gcr.io/distroless/base-debian12:nonroot
COPY --from=build /src/target/magix /magix
EXPOSE 8888
ENTRYPOINT ["/magix"]
