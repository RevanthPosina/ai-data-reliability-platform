ARG MAVEN_IMAGE
ARG DEBEZIUM_VERSION
FROM ${MAVEN_IMAGE} AS deps
ARG CONFLUENT_CONVERTER_VERSION
WORKDIR /build
COPY pom.xml .
RUN mvn -B -q -Dconfluent.version=${CONFLUENT_CONVERTER_VERSION} \
      dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=/out

FROM quay.io/debezium/connect:${DEBEZIUM_VERSION}
# Separate plugin dir so Connect isolates the converter's classpath from the connector's.
COPY --from=deps /out/ /kafka/connect/confluent-avro-converter/
