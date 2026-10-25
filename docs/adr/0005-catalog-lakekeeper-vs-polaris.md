# ADR-0005: Iceberg REST catalog: Lakekeeper vs Apache Polaris
- Status: accepted (G0). Verified 2026-10-25.
## Evidence
- Apache Polaris graduated to an Apache top-level project on 2026-02-19 (polaris.apache.org blog). Latest release 1.8.0 (2026-09-28). JVM/Quarkus service.
- Lakekeeper: Rust, single small container, uses the existing Postgres as its backend. 0.13.x docs support `flavor: s3-compat`, `path-style-access`, `sts-enabled: false`, `remote-signing-enabled`.
## Decision
**Lakekeeper v0.13.6** (backed by a separate `lakekeeper` database in the source Postgres instance).
Version policy exception: 0.14.0 (2026-10-07) has breaking authn/authz changes and was followed by two patch releases within 18 days.
Storage profile: `s3-compat`, `path-style-access: true`, `sts-enabled: false`, `remote-signing-enabled: false`. Spark and pyiceberg use static S3 keys (client-managed). Remote signing is the fallback.
## Consequences
- Lower RAM than Polaris on a 16 GB laptop.
- Polaris is the enterprise answer to "which catalog would you run at a company"; the REST spec keeps the switch cheap.
- Sharing the Postgres instance means a Postgres outage takes out the catalog. Accepted for a single-node demo.
