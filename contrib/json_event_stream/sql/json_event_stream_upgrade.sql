CREATE EXTENSION json_event_stream WITH VERSION '1.0';

SELECT extversion
FROM pg_extension
WHERE extname = 'json_event_stream';

SELECT json_event_stream_create('legacy_events', '1 day', '30 days', false);

ALTER EXTENSION json_event_stream UPDATE TO '1.1';

SELECT extversion
FROM pg_extension
WHERE extname = 'json_event_stream';

SELECT stream_schema, stream_name
FROM json_event_streams
WHERE stream_table = 'legacy_events'::regclass;

SELECT json_event_stream_ingest(
	'legacy_events'::regclass,
	'legacy',
	'factory/legacy',
	'{"legacy": true}'::jsonb,
	'{}'::jsonb,
	NULL,
	'2026-06-24 13:00:00+00'::timestamptz);

SELECT topic, payload
FROM legacy_events;

DROP EXTENSION json_event_stream CASCADE;

\echo json_event_stream_upgrade_done
