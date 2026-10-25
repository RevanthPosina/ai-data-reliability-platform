ARG MAVEN_IMAGE
ARG SPARK_IMAGE
FROM ${MAVEN_IMAGE} AS jars
ARG ICEBERG_VERSION
ARG SPARK_VERSION
ARG SCALA_BINARY
ARG SPARK_KAFKA_CLIENTS_VERSION
ARG SPARK_COMMONS_POOL2_VERSION
WORKDIR /build
COPY docker/spark/pom.xml pom.xml
RUN mvn -B -q -Diceberg.version=${ICEBERG_VERSION} -Dspark.version=${SPARK_VERSION} \
      -Dscala.binary=${SCALA_BINARY} -Dkafka.version=${SPARK_KAFKA_CLIENTS_VERSION} \
      -Dpool2.version=${SPARK_COMMONS_POOL2_VERSION} \
      dependency:copy-dependencies -DexcludeTransitive=true -DoutputDirectory=/out

FROM ${SPARK_IMAGE}
ARG SPARK_FASTAVRO_VERSION
USER root
RUN pip install --no-cache-dir fastavro==${SPARK_FASTAVRO_VERSION}
COPY --from=jars /out/*.jar /opt/spark/jars/
COPY src/adrp /opt/app/adrp
# Named volume mounted here inherits this ownership on first creation.
RUN mkdir -p /checkpoints && chown spark /checkpoints
ENV PYTHONPATH=/opt/app:/opt/spark/python \
    AWS_REGION=us-east-1
WORKDIR /opt/app
USER spark
