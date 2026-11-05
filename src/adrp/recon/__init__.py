"""Reconciliation logic (G10a): canonical row hashing, Postgres-vs-silver comparison, consistent-cut and offset
checks, per-key continuous judging, and fault outcome classification.

Pure logic: stdlib plus xxhash. No Docker, no network. The adapters that read the live stack are in
scripts/reconcile.py. Result objects expose counts, bucket numbers and hex checksums only (never row ids)."""
