# ADR-0008: dbt on Trino vs dbt on Spark
- Status: accepted (G0)
## Decision
**dbt-core 1.12.5 + dbt-trino 1.10.6 on Trino 483** for gold (Milestone B).
## Reasons
- Trino reads Iceberg through the same Lakekeeper REST catalog, with no Thrift server to run (dbt-spark needs one).
- dbt-trino supports snapshots and model contracts on Iceberg. Only `not_null` is enforced by Trino/Iceberg; other constraints are metadata-only (stated in G4).
- Trino is in its own profile (`transform`, +3 GB), so the core profile stays within laptop limits.
## Consequences
- Gold reads format-version 2 tables (ADR-0004).
- CI runs `dbt build` on a fixture project against a Trino service container (memory catalog), so the adapter path is exercised from G0.
