# Install / update log

Everything installed, pulled, updated or removed on this machine for this project, in order.
Version policy (ADR-0000): stable, non-prerelease, released on or before 2026-10-11. That includes transitive Python deps, held by constraints files generated with `scripts/pin_policy.py`.

## 2026-10-25 (G0)

| Action | What | Notes |
|---|---|---|
| started | Docker Desktop (already installed; engine 20.10.22, Compose 2.15.1, VM 8 GB / 4 CPU) | not upgraded |
| **pulled (unintended)** | `apache/spark:4.1.3-scala2.13-java17-python3-ubuntu` | A `docker run --version` check pulled it, the host disk filled (237 MiB free) and the Docker VM went read-only. Docker was quit. |
| removed (user-approved) | 12 stopped containers from another project: `spark-master`, `spark-worker-1..3`, `kafka-broker-1..3`, `kafka-controller-1..3`, `schema-registry`, `redpanda-console` | Host free disk went from 0.3 to 23 GiB. That project's images and its 22 volumes were left untouched. |
| removed | the accidental Spark image above | |
| `git init` | project-local repo in `DelegationGarden/` | The parent `Projects/` repo had no commits; this keeps the project separate. |
| created venv | `.venv/` (host Python 3.12.7) from `requirements-dev.txt` + `requirements-dev.constraints.txt` | ruff 0.17.0, mypy 2.4.0, pytest 9.1.1, hypothesis 6.169.0, fastavro 1.13.1, dbt-core 1.12.5, dbt-trino 1.10.6 + transitive deps (all within policy). Recreated once to apply the constraints (10 transitive packages had resolved past the cutoff). |
| pulled (build-only) | `maven:3.9.16-eclipse-temurin-17` | Builder stage for the connect and spark images. The `-alpine` variant was tried first and has no arm64 build. |
| pulled (build base) | `python:3.12.14-slim-bookworm`, `quay.io/debezium/connect:3.7.0.Final`, `apache/spark:4.1.3-scala2.13-java17-python3-ubuntu` | |
| built | `adrp/connect:3.7.0.Final` (1.57 GB) | + `io.confluent:kafka-connect-avro-converter:8.3.2` and its runtime deps (avro 1.12.1, kafka-schema-registry-client 8.3.2, guava 32.0.1-jre, jackson 2.20 …) in `/kafka/connect/confluent-avro-converter/` |
| built | `adrp/spark:4.1.3-iceberg1.12.0` (1.42 GB) | + iceberg-spark-runtime-4.1_2.13 1.12.0, iceberg-aws-bundle 1.12.0, spark-sql-kafka-0-10_2.13 4.1.3, spark-token-provider-kafka-0-10_2.13 4.1.3, kafka-clients 3.9.1, commons-pool2 2.12.1; pip `fastavro==1.12.2` (the image ships Python 3.10.12, and fastavro 1.13 requires ≥ 3.11) |
| built | `adrp/app:dev` (1.87 GB) | torch 2.14.1+cpu, sentence-transformers 6.1.0, transformers 5.19.0, dagster 1.13.26, pyiceberg 0.12.0, qdrant-client 1.19.1, psycopg 3.3.6, boto3 1.43.111. Full set: `docker/app/requirements.lock`. Model `all-MiniLM-L6-v2@1110a243` (PyTorch weights + tokenizer only) baked in. An earlier 3.65 GB build downloaded every weight format and was replaced. |
| pulled | `postgres:17.10-alpine`, `redpandadata/redpanda:v26.2.4`, `chrislusf/seaweedfs:4.48`, `quay.io/lakekeeper/catalog:v0.13.6`, `qdrant/qdrant:v1.19.2` | core profile |
| pulled | `trinodb/trino:483` | transform profile (dbt fixture) |
| removed | 4 untagged intermediate images from my own rebuilds | disk |
| created | Docker volumes `adrp_pg-data`, `adrp_rp-data`, `adrp_s3-data`, `adrp_qdrant-data`, `adrp_spark-checkpoints` (recreated once for ownership), `adrp_embedder-state` | `make clean-volumes` removes them |

Host disk after G0: ~11 GiB free.

## 2026-10-25 (G1)

| Action | What | Notes |
|---|---|---|
| pip (dev venv) | `faker==40.43.0`, `numpy==2.5.4` | `requirements-dev.constraints.txt` regenerated; 0 packages past the cutoff |
| rebuilt | `adrp/app:dev` + `Faker==40.43.0` | lock regenerated; 0 packages past the cutoff; old image removed |
| wiped + recreated | this project's Docker volumes (`make clean-volumes`) | schema changed (G1 domain); data was test-only |
| changed | SeaweedFS `mem_limit` 384m → 512m | observed 357 MiB under generator load (after the max-rate run) |
| created | `runs/data/` (git-ignored) | generator Parquet logs |

## 2026-10-27 (G2)

| Action | What | Notes |
|---|---|---|
| pip (dev venv) | `confluent-kafka==2.16.0`, `PyYAML==6.0.3`, `psycopg[binary]==3.3.6` | host-side Kafka consumer for the G2 check; YAML for contracts; psycopg for the exporter's unit tests. Constraints regenerated, 0 packages past the cutoff. |
| rebuilt | `adrp/app:dev` | now includes `adrp.obs.exporter`, `adrp.contracts`; no new app dependencies |
| wiped + recreated | this project's Docker volumes (`make g2-check` does this on purpose) | a real snapshot needs empty volumes; data was test-only |
| pulled | none | `make contracts-ci` runs a throwaway container from the already-present Redpanda image and removes it |
| added | Compose service `exporter` (apps profile, `127.0.0.1:9108`, 128 MB) | slot lag and connector status metrics |
