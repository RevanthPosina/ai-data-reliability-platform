# ADR-0011: Serialization: Avro with the Confluent AvroConverter
- Status: accepted (G0). Verified 2026-10-25.
## Evidence
The `quay.io/debezium/connect` 3.7 image bundles Apicurio converters 3.2.5 (`connect-base/3.7/Dockerfile`, `APICURIO_ENABLED=yes`) and does **not** include Confluent's AvroConverter (dropped from the image since 2.0, DBZ-4935). Apicurio supports a Confluent-compatible wire format (`as-confluent`) since 3.2.0.Alpha1 (DBZ-8945).
## Decision
As in PLAN.md: a custom image `FROM quay.io/debezium/connect:3.7.0.Final` adds `io.confluent:kafka-connect-avro-converter:8.3.2`, with dependencies resolved by Maven in a build stage. It registers schemas in Redpanda's Schema Registry (Confluent-compatible API) with BACKWARD compatibility per subject. JSON is not used.
## Consequences
- The wire format is the standard 5-byte Confluent header (magic + schema id), which bronze parses into `schema_id`.
- Apicurio with `as-confluent: true` is the documented fallback if the Confluent JARs cause classloading conflicts.
