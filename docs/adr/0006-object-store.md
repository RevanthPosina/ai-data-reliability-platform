# ADR-0006: Object store
- Status: accepted (G0). Verified 2026-10-25.
## Evidence
MinIO CE: official binaries and Docker images stopped in Oct 2025; README changed to source-only and "no longer maintained"; repository archived in April 2026 (glukhov.org "MinIO CE in 2026"; vonng.com "MinIO is dead"). Using a frozen last image would carry unpatched CVEs.
## Options
- SeaweedFS: one container (`weed server -s3`), static identities from a JSON file, multi-arch images, frequent releases.
- Garage: also light. It needs a layout-assign step and key creation through its CLI before first use, which adds bootstrap steps.
## Decision
**SeaweedFS 4.48** (`chrislusf/seaweedfs:4.48`), S3 path-style, static keys, single bucket `lake`.
## Consequences
- G8 physical-absence verification lists objects via the S3 API and scans Parquet. That works the same on any S3 store.
- Garage stays documented as the swap-in.
