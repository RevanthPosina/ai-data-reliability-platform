# ADR-0010: Reproducibility vs deletability
- Status: accepted (G0); exercised in G7/G8
## Tension
"Rebuild dataset v{n} bit-for-bit" needs its source data kept forever. "Forget customer X" needs that data gone.
## Decision
- Reproducibility is claimed **only within retention window R**: rebuilding v{n} from its recorded source snapshot ids reproduces its content hash while those snapshots are retained. After R, the materialized table is the record.
- A forget request touching v{n} **invalidates** it: rebuild as v{n}.1 with the exclusion recorded in the dataset card, then drop v{n}. v{n} is never again called reproducible.
- Deletion wins over reproducibility.
## Consequences
Dataset cards carry R, source snapshot ids and any exclusions. The must-never-claim list forbids "reproducible" for invalidated versions or past R.
