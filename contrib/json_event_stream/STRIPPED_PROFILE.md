# JSON Event Stream Stripped Profile

This profile keeps the normal PostgreSQL core build and narrows `contrib` to the
JSON event ingestion package:

- `json_event_stream` extension SQL and control files
- `json_event_ctl`
- `json_event_httpd`
- `json_event_mqttd`
- `json_event_bench`

Core PostgreSQL remains intact, including `jsonb`, jsonpath, WAL, heap storage,
partitioning, indexes, transactions, `psql`, and libpq.

## Make

Build the stripped JSON event distribution:

```sh
make json-event-stripped-world
```

Install it:

```sh
make json-event-stripped-install
```

For contrib-only iteration:

```sh
make -C contrib JSON_EVENT_STREAM_PROFILE=1 all
make -C contrib JSON_EVENT_STREAM_PROFILE=1 install
```

The profile intentionally excludes broad optional contrib modules such as FDWs,
procedural-language helpers, XML/text extras, and administrative extensions that
are not part of the v1 immutable JSON event-store surface.

## Meson

Configure with the event-stream contrib profile:

```sh
meson setup build-json-event -Dcontrib_profile=json_event_stream
ninja -C build-json-event
```

The default Meson profile remains `full`, so ordinary PostgreSQL builds continue
to include all configured contrib packages.
