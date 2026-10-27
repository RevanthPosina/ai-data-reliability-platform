ARG MAVEN_IMAGE
ARG SPARK_IMAGE
FROM ${MAVEN_IMAGE} AS jars
ARG ICEBERG_VERSION
ARG SPARK_VERSION
ARG SCALA_BINARY
ARG SPARK_KAFKA_CLIENTS_VERSION
ARG SPARK_COMMONS_POOL2_VERSION
ARG OPENLINEAGE_VERSION
WORKDIR /build
COPY docker/spark/pom.xml pom.xml
RUN mvn -B -q -Diceberg.version=${ICEBERG_VERSION} -Dspark.version=${SPARK_VERSION} \
      -Dscala.binary=${SCALA_BINARY} -Dkafka.version=${SPARK_KAFKA_CLIENTS_VERSION} \
      -Dpool2.version=${SPARK_COMMONS_POOL2_VERSION} -Dopenlineage.version=${OPENLINEAGE_VERSION} \
      dependency:copy-dependencies -DexcludeTransitive=true -DoutputDirectory=/out

FROM ${SPARK_IMAGE}
USER root
# The image ships Python 3.10. Dagster runs here too, so maintenance jobs can call Iceberg procedures.
# constraints.txt holds every transitive package to the >=14-day policy (scripts/pin_policy.py).
COPY docker/spark/requirements.txt docker/spark/constraints.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt -c /tmp/constraints.txt
COPY --from=jars /out/*.jar /opt/spark/jars/
COPY src/adrp /opt/app/adrp
COPY src/adrp_orch /opt/app/adrp_orch
# Named volumes mounted here inherit this ownership on first creation.
RUN mkdir -p /checkpoints /eventlog /opt/dagster_home && chown spark /checkpoints /eventlog /opt/dagster_home
ENV PYTHONPATH=/opt/app:/opt/spark/python \
    AWS_REGION=us-east-1
WORKDIR /opt/app
USER spark
