# ADR-0009: Materialized dataset tables vs Iceberg tags on silver
- Status: accepted (G0); implemented in G7
## Decision
Each training/eval dataset version is materialized into its own table `ml.dataset_v{n}` plus a Parquet export and a dataset card.
## Why not tags
A tag on a silver snapshot pins that snapshot and every file it references. `expire_snapshots` cannot remove it, so a forget request cannot physically remove the customer from silver while the tag exists. Materialized tables isolate retention: forgetting rewrites or drops one dataset table without blocking silver maintenance.
## Consequences
Extra storage per version. Reproducibility from source snapshots holds only within retention window R (ADR-0010).
