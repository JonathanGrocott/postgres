# JSON Event Stream TODO

## Done

- [x] Harden the extension API with versioned upgrade packaging, schema-qualified stream creation, relation collision checks, basic permission guards, advisory-lock partition creation, and expanded regression coverage.
- [x] Add real HTTP ingestion with a bundled libpq-backed `json_event_httpd` daemon for single-event and batch JSON POST routes, bearer-token checks, body-size validation, and dead-letter routing for failed ingest attempts.
- [x] Add real MQTT ingestion with a bundled libpq-backed `json_event_mqttd` daemon for DB-backed source config, MQTT 3.1.1 subscribe, reconnect/backoff, QoS 1 acknowledgements, JSON ingest, dead-letter routing, and source error telemetry.
- [x] Make stream management feel native with `json_event_ctl`, a fork-native command layer for extension install, stream creation, MQTT/HTTP source creation, retention changes, and status inspection.
- [x] Add performance and storage tuning benchmarks with `json_event_bench` for append throughput, topic/time/jsonpath queries, retention dry-runs, table/index bytes, and payload GIN write-amplification signals.
- [x] Create a stripped distribution profile that keeps core Postgres and `json_event_stream` while excluding broad optional contrib modules via `make json-event-stripped-world` / `json-event-stripped-install`, Meson `-Dcontrib_profile=json_event_stream`, and the `STRIPPED_PROFILE.md` operator note.
- [x] Package the stripped JSON event build into a release artifact with `json_event_package`, operator docs in `RELEASE.md`, and installed smoke-test automation via `json_event_smoke`.
- [x] Add local CI/release workflow coverage with `json_event_ci` and `.github/workflows/json-event-stream.yml` for the stripped profile build, package artifact, smoke test, SQL regressions, and benchmark sanity run.
- [x] Prepare clean-fork handoff notes with `FORK_HANDOFF.md`, remote hygiene guardrails, branch policy, first local release checklist, and `json_event_remote_guard`.

## Next

- [ ] Add a fork branding pass: version string, package naming, service names, README entry point, and default operator paths for `postgres-json-event`.
