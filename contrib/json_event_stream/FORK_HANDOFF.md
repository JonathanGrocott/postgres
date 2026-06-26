# JSON Event Stream Clean Fork Handoff

This work is local-only until a clean fork is prepared. Do not push this branch
or any generated release artifacts to `github.com/postgres/postgres`.

## Fork Name

Recommended repository name:

```text
postgres-json-event
```

Working product name:

```text
JSON Event Postgres
```

The repo should remain recognizable as PostgreSQL, but the fork identity should
make the event-store scope obvious in remotes, release artifacts, and operator
docs.

## Remote Hygiene

Current upstream remote in this checkout:

```sh
git remote -v
```

Expected current state before fork setup:

```text
origin  https://github.com/postgres/postgres.git (fetch)
origin  https://github.com/postgres/postgres.git (push)
```

Before any push, change the writable remote away from upstream:

```sh
git remote rename origin upstream
git remote add origin git@github.com:YOUR_ORG/postgres-json-event.git
git remote set-url --push upstream DISABLED
```

Then verify:

```sh
contrib/json_event_stream/json_event_remote_guard \
  --expect-substring postgres-json-event
```

The guard fails while `origin` still points at upstream PostgreSQL.

## Branch Policy

Use local branches until the clean fork remote exists.

Recommended branch names:

```text
dev/json-event-bootstrap
dev/json-event-release-ci
release/json-event-v0.1.0
```

Keep upstream PostgreSQL history intact. Do not rewrite imported upstream
history. Put fork-specific work on explicit `dev/json-event-*` or
`release/json-event-*` branches.

## First Local Release Checklist

1. Confirm remotes are safe:

   ```sh
   git remote -v
   contrib/json_event_stream/json_event_remote_guard \
     --expect-substring postgres-json-event
   ```

2. Run the local release gate:

   ```sh
   contrib/json_event_stream/json_event_ci \
     --output-dir /tmp/postgres-json-event-release \
     --rows 100 \
     --keep-artifacts
   ```

3. Inspect generated artifacts:

   ```sh
   ls -lh /tmp/postgres-json-event-release
   tar -tzf /tmp/postgres-json-event-release/postgres-json-event-ci.tar.gz |
     grep json_event
   ```

4. Smoke the staged install or extracted artifact:

   ```sh
   /tmp/postgres-json-event-release/install/usr/local/pgsql/bin/json_event_smoke \
     --bindir /tmp/postgres-json-event-release/install/usr/local/pgsql/bin
   ```

5. Record the release evidence:

   - git revision
   - `json_event_ci.BUILDLOG`
   - package `BUILDINFO`
   - package `MANIFEST`
   - smoke-test output
   - benchmark row count and output location

## What Not To Publish

Do not publish these to upstream PostgreSQL:

- local feature branch
- `.github/workflows/json-event-stream.yml`
- generated package archives
- release notes for this fork
- issue or PR traffic about this local experiment

When the clean fork exists, publish only to that fork-owned remote.
