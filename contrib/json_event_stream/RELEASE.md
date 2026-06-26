# JSON Event Stream Release Packaging

The stripped release artifact is created from a configured PostgreSQL build tree
with the JSON event profile enabled through the Make targets added for this fork.

## Build Artifact

From the repository root:

```sh
contrib/json_event_stream/json_event_package --name postgres-json-event-dev
```

Useful options:

```sh
contrib/json_event_stream/json_event_package \
  --output-dir /tmp/json-event-artifacts \
  --prefix /opt/postgres-json-event \
  --name postgres-json-event-dev \
  -j 8
```

The package step runs `make json-event-stripped-world`, installs into a staging
directory with `make json-event-stripped-install DESTDIR=...`, and writes:

- `NAME.tar.gz`
- `NAME.MANIFEST`
- `NAME.BUILDINFO`
- `NAME.BUILDLOG`

The archive includes the normal PostgreSQL server/client install plus the JSON
event extension, ingestion daemons, management CLI, benchmark runner, and smoke
test script.

## Smoke Test

After installing or extracting the artifact, run:

```sh
bin/json_event_smoke --bindir bin
```

The smoke test starts a temporary cluster, installs `json_event_stream`, creates
a stream and HTTP source, runs `json_event_bench`, checks stream/source/event
counts, and removes the cluster unless `--keep` is provided.

For a staged package rooted at `postgres-json-event-dev`:

```sh
postgres-json-event-dev/opt/postgres-json-event/bin/json_event_smoke \
  --bindir postgres-json-event-dev/opt/postgres-json-event/bin
```

## Local CI Gate

Run the full stripped-profile release gate locally:

```sh
contrib/json_event_stream/json_event_ci -j 4 --rows 100 --keep-artifacts
```

This builds `json-event-stripped-world`, runs the extension SQL regressions,
creates a package artifact, installs the stripped profile into a temporary
prefix, runs `json_event_smoke`, and checks the benchmark runner.

The fork workflow at `.github/workflows/json-event-stream.yml` calls the same
script. It is intended for a clean fork and should not be enabled against the
upstream PostgreSQL repository.

## Clean Fork Guard

Before any future push or release upload, verify the writable remote does not
point at upstream PostgreSQL:

```sh
contrib/json_event_stream/json_event_remote_guard \
  --expect-substring postgres-json-event
```

See `FORK_HANDOFF.md` for the local-only fork setup checklist.
