# ADR-0004: Iceberg table format version
- Status: accepted (G0). Verified 2026-10-25.
## Evidence
- Trino 483/484 Iceberg connector docs: "The connector supports Apache Iceberg table spec versions 1 and 2. Support for format version 3 is experimental." Default for new tables is 2.
- Spark 4.1 + Iceberg 1.12.0: v3 (deletion vectors) supported.
- pyiceberg 0.12.0 release notes: Puffin deletion-vector reading and a DeleteFileIndex for position deletes.
## Decision
**format-version = 2** for every table (position deletes for MoR). Set explicitly at table creation (`'format-version'='2'`) and checked by an asset check.
## Consequences
- Every engine in the stack (Spark, Trino for dbt, pyiceberg for checks) reads and writes the tables with non-experimental support.
- Revisit once Trino drops the "experimental" label for v3. Deletion vectors would reduce delete-file count, which is relevant to G12.
