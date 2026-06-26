CREATE EXTENSION json_event_stream;

CREATE TABLE collision(id int);
SELECT json_event_stream_create('collision');

SELECT json_event_stream_create('events', '1 day', '7 days', true);

CREATE SCHEMA ingest;
SELECT json_event_stream_create('ingest', 'web_events', '1 hour', '1 day', false);

SELECT json_event_mqtt_source_create(
	'line1',
	'mqtts://broker.example.com',
	'factory/line1/#',
	'events'::regclass);

SELECT json_event_http_source_create(
	'webhook',
	'/events',
	'events'::regclass);

SELECT json_event_stream_ingest(
	'events'::regclass,
	'line1',
	'factory/line1/temperature',
	'{"temperature": 82, "unit": "F"}'::jsonb,
	'{"qos": 1}'::jsonb,
	'2026-06-24 12:00:00+00'::timestamptz,
	'2026-06-24 12:00:01+00'::timestamptz);

SELECT json_event_stream_ingest(
	'ingest.web_events'::regclass,
	'webhook',
	'/events',
	'{"ok": true}'::jsonb,
	'{}'::jsonb,
	NULL,
	'2026-06-24 12:30:00+00'::timestamptz);

SELECT json_event_stream_ingest(
	'events'::regclass,
	'',
	'factory/line1/temperature',
	'{"temperature": 82}'::jsonb);

SELECT topic, payload @? '$.temperature ? (@ > 80)' AS hot
FROM events
WHERE received_at >= '2026-06-24 00:00:00+00'::timestamptz;

SELECT source, topic, payload
FROM ingest.web_events;

SELECT json_event_stream_reject(
	'events'::regclass,
	'line1',
	'factory/line1/bad',
	'{bad json',
	'invalid_json',
	'payload could not be parsed');

SELECT source_name, source_type, enabled
FROM json_event_ingestion_status
ORDER BY source_name;

SELECT source, topic, error_code
FROM json_event_rejected_messages;

SELECT stream_table, partition_table IS NOT NULL AS has_partition
FROM json_event_partition_status
ORDER BY partition_table::text;

SELECT json_event_stream_set_retention('events'::regclass, '100 years');

SELECT bool_or(would_drop) AS would_drop_any
FROM json_event_stream_apply_retention('events'::regclass, true);

SELECT stream_table, recommendation
FROM json_event_index_recommendations;

SELECT json_event_stream_add_payload_path_index(
	'events'::regclass,
	'events_temperature_idx',
	'temperature');

SELECT indexname
FROM pg_indexes
WHERE tablename = 'events'
ORDER BY indexname;

\echo json_event_stream_done
