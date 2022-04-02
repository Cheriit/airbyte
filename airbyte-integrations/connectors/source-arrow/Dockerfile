FROM airbyte/integration-base-java:dev AS build

WORKDIR /airbyte

ENV APPLICATION source-arrow

COPY build/distributions/${APPLICATION}*.tar ${APPLICATION}.tar

RUN tar xf ${APPLICATION}.tar --strip-components=1 && rm -rf ${APPLICATION}.tar

FROM airbyte/integration-base-java:dev

WORKDIR /airbyte

ENV APPLICATION source-arrow

COPY --from=build /airbyte /airbyte
## TODO Add your dockerfile instructions here
## TODO uncomment the below line
# ENV AIRBYTE_ENTRYPOINT="update this with the command you use for an entrypoint"

# Airbyte's build system uses these labels to know what to name and tag the docker images produced by this Dockerfile.
LABEL io.airbyte.name=airbyte/source-arrow
LABEL io.airbyte.version=0.1.0
