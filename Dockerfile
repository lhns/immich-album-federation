FROM eclipse-temurin:21-jdk AS build
ARG SBT_VERSION=1.10.11
RUN curl -fsSL "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" \
      | tar xz -C /opt \
 && ln -s /opt/sbt/bin/sbt /usr/local/bin/sbt

WORKDIR /build
# Dependency resolution as its own layer: source changes don't re-download the world.
COPY build.sbt ./
COPY project/build.properties project/plugins.sbt project/
RUN sbt -batch update
COPY src/ src/
RUN sbt -batch assembly

FROM eclipse-temurin:21-jre
RUN useradd --system --create-home --home-dir /app sync
USER sync
WORKDIR /app
COPY --from=build /build/target/scala-3.7.3/app.jar /app/app.jar

# Configuration comes from environment variables (see doc/sync.local.env.example) and
# an optional mounted config file, e.g.:
#   docker run --env-file sync.env -e DRY_RUN=true -v ./sync.yaml:/app/sync.yaml \
#     ghcr.io/lhns/immich-album-federation --config=/app/sync.yaml
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
