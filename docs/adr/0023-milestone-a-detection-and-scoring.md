# ADR-0023: Milestone A detection and scoring
- Status: accepted (G10a); written before the first run, so nothing here is a result. Items marked **unverified** are not yet observed.
## Decision
Each fault is scored against the **unedited** design §5 matrix. The expected columns are copied verbatim into a `PREREGISTERED` constant, and any change in expected behaviour is a dated note in design §5 and in the owning ADR, never a rewritten row or a loosened check.
## Detection in Milestone A
Prometheus and alerting do not exist yet (design S13 assigns alerts to G13). Until then, a "connector-status alert" or "slot-lag alert" is a **polled exporter check**: a sampler thread reads `adrp.obs.exporter` on `:9108/metrics` every 2 s through `g2._try`. A connector-status check **fired** when `adrp_connector_up == 0` **or** `adrp_exporter_source_up{source="connect"} == 0`. The second clause is needed because while Connect is down the `adrp_connector_up` series is absent.
- The slot-lag threshold stays at design S13's 1 GiB. A short F5 outage is not expected to breach it, so the record is the observed peak and whether lag grew. The SLO is not lowered and no breach is claimed.
- A check that did not fire when the fault's expected outcome was "self-healed" is recorded as `expected_detection_fired: false`. It is not a failure, and it is not rewritten as a pass. The outage is not lengthened to make an alert fire.
- F6b names "registry compatibility check (CI)". The G10a harness sets the subject's compatibility to `NONE` to bypass the registry, so F6b is scored on silver's quarantine count and the `SCHEMA_INCOMPATIBLE` rows, and the manifest states: registry gate bypassed; silver's gate tested; `contracts-ci` covers the registry separately. F6b is never scored as caught by the registry.
- F13 stays a row covered by the named check, the `__debezium_unavailable_value` count at every gate. It is reported in the fault table and is not a seventh fault.
## Classification
`classify(recon_ok_after, fired)`:
| Reconciler clean after recovery | A named check fired | Outcome |
|---|---|---|
| yes | yes | `CAUGHT` |
| yes | no | `SELF_HEALED` |
| no | no | `SILENT` |
The PLAN does not classify a non-clean reconciler with a fired check; G10a reports it as a failed gate together with the check that fired, and it fails the run. Policy:
- F6b must be `CAUGHT`.
- Any `SILENT` fails the run.
- "Self-healed" always means self-healed in this run. F4's retry is tested without concurrent writers (ADR-0020), and that is stated wherever F4 is reported.
- The reconciler gate (ADR-0021) after each fault has a fixed set of named checks prefixed by the fault label, so a failure names the pair that differed.
## Wording enforcement
`wording_violations` runs over every recorded check string and the rendered table before the manifest is written, and has a unit test over the strings. It rejects the claims the PLAN forbids:
- Exactly-once claims. The wording is "idempotent under the tested faults". "No duplicates" is limited to silver and always accompanied by "bronze holds duplicates by design".
- "Deleted" without "logical" or "physical". Examples: "logical delete (silver tombstone)", "physically removed" for orphan removal.
- "0 silent" without its scope. The text is "0 silent corruption detected by the reconciler", and a fixed `scope_note` in the manifest and the rendered table reads: "Zero mismatches under the 6 tested faults at this cut; undetected faults may exist."
- Production-scale or durability claims. Reports give row counts and "one machine, N runs"; F5 is a graceful restart on a `dev-container` broker and supports no durability claim.
- Compliance wording; orphan removal and inventory rows stay "deletion propagation verified in this system", and only where relevant.
- Lineage: the txid-to-LSN mapping is a reconciliation key and is not called lineage.
- "Reproducible" for the silver-to-bronze rebuild, which is an equality check on this table.
## What the manifest keeps for G11a
`result.faults.<id>` has stable keys so runs can be aggregated: `dupes, losses, resurrections, injected_at, recovered_s, outcome, expected_detection_fired`. The manifest also records `topic_mode`, `run_ids`, and each load's `achieved_changes_per_s` and `fell_behind`. The txid-to-LSN mapping lives in `recon/continuous.py` so G11a can reuse it. The fault table in `docs/results/` is rendered from the manifest, not typed.
## Limits
- Six faults, one machine, and a small number of runs. The table reports what happened in those runs.
- A polled sampler can miss a short outage between samples; "did not fire" means "not observed by the sampler".
- Two blind faults (F11, F12) and the Milestone B rows are outside G10a; G5 adds the remaining checks and re-scores the matrix.
- Whether the expected detections for F4 and F5 fire in a given run is **unverified** until the first run; the policy above is written so that either result is reported honestly.
