/*
 * json_event_mqttd
 *
 * Small libpq-backed MQTT subscriber for json_event_stream.
 */
#include "postgres_fe.h"

#include "common/logging.h"
#include "common/string.h"
#include "getopt_long.h"
#include "lib/stringinfo.h"
#include "libpq-fe.h"
#include "pg_getopt.h"

#include <ctype.h>
#include <errno.h>
#include <netdb.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#define DEFAULT_CLIENT_ID "json_event_mqttd"
#define DEFAULT_KEEPALIVE 60
#define DEFAULT_RECONNECT_SECONDS 5
#define MQTT_MAX_PACKET (16 * 1024 * 1024)

typedef struct
{
	char	   *conninfo;
	char	   *source_name;
	char	   *broker_uri;
	char	   *topic_filter;
	char	   *stream_table;
	char	   *client_id;
	int			reconnect_seconds;
	bool		once;
} MqttOptions;

typedef struct
{
	char	   *host;
	char	   *port;
} BrokerAddress;

static volatile sig_atomic_t shutdown_requested = 0;

static void usage(const char *progname);
static void parse_options(int argc, char **argv, MqttOptions *opts);
static void load_source_config(PGconn *conn, MqttOptions *opts);
static BrokerAddress parse_broker_uri(const char *uri);
static int	connect_tcp(const char *host, const char *port);
static bool mqtt_connect(int fd, const char *client_id);
static bool mqtt_subscribe(int fd, const char *topic_filter);
static bool mqtt_read_packet(int fd, unsigned char *packet_type,
							 unsigned char **packet, size_t *packet_len);
static bool mqtt_write_all(int fd, const unsigned char *buf, size_t len);
static void mqtt_write_remaining_length(StringInfo out, size_t len);
static bool mqtt_read_remaining_length(int fd, size_t *len);
static bool mqtt_handle_publish(int fd, PGconn *conn, const MqttOptions *opts,
								unsigned char packet_type,
								const unsigned char *packet, size_t packet_len);
static bool sql_ingest(PGconn *conn, const MqttOptions *opts, const char *topic,
					   const char *payload, char **error_message);
static void sql_reject(PGconn *conn, const MqttOptions *opts, const char *topic,
					   const char *payload, const char *error_code,
					   const char *error_message);
static void sql_set_error(PGconn *conn, const char *source_name,
						  const char *error_message);
static char *json_escape(const char *input);
static char *build_headers_json(const MqttOptions *opts, const char *topic);
static void signal_handler(SIGNAL_ARGS);

static void
usage(const char *progname)
{
	printf("%s subscribes to MQTT JSON messages and ingests them into json_event_stream.\n\n", progname);
	printf("Usage:\n");
	printf("  %s --source=NAME [OPTION]...\n", progname);
	printf("  %s --broker=mqtt://HOST[:PORT] --topic=FILTER --stream=REGCLASS [OPTION]...\n\n", progname);
	printf("Options:\n");
	printf("  -d, --dbname=CONNINFO      libpq connection string (default: dbname=postgres)\n");
	printf("  -s, --source=NAME          load broker/topic/stream from json_event_sources\n");
	printf("  -b, --broker=URI           MQTT broker URI, plain mqtt:// only\n");
	printf("  -T, --topic=FILTER         MQTT topic filter to subscribe to\n");
	printf("  -S, --stream=REGCLASS      target json event stream\n");
	printf("  -c, --client-id=ID         MQTT client id (default: %s)\n", DEFAULT_CLIENT_ID);
	printf("  -r, --reconnect=SECONDS    reconnect delay (default: %d)\n", DEFAULT_RECONNECT_SECONDS);
	printf("  -1, --once                 exit after first publish is handled\n");
	printf("  -V, --version              output version information, then exit\n");
	printf("  -?, --help                 show this help, then exit\n");
}

static void
parse_options(int argc, char **argv, MqttOptions *opts)
{
	static const struct option long_options[] = {
		{"broker", required_argument, NULL, 'b'},
		{"client-id", required_argument, NULL, 'c'},
		{"dbname", required_argument, NULL, 'd'},
		{"once", no_argument, NULL, '1'},
		{"reconnect", required_argument, NULL, 'r'},
		{"source", required_argument, NULL, 's'},
		{"stream", required_argument, NULL, 'S'},
		{"topic", required_argument, NULL, 'T'},
		{"version", no_argument, NULL, 'V'},
		{"help", no_argument, NULL, '?'},
		{NULL, 0, NULL, 0}
	};
	int			c;
	const char *progname = get_progname(argv[0]);

	opts->conninfo = pg_strdup("dbname=postgres");
	opts->source_name = NULL;
	opts->broker_uri = NULL;
	opts->topic_filter = NULL;
	opts->stream_table = NULL;
	opts->client_id = pg_strdup(DEFAULT_CLIENT_ID);
	opts->reconnect_seconds = DEFAULT_RECONNECT_SECONDS;
	opts->once = false;

	while ((c = getopt_long(argc, argv, "1b:c:d:r:s:S:T:V?", long_options, NULL)) != -1)
	{
		switch (c)
		{
			case '1':
				opts->once = true;
				break;
			case 'b':
				opts->broker_uri = pg_strdup(optarg);
				break;
			case 'c':
				opts->client_id = pg_strdup(optarg);
				break;
			case 'd':
				opts->conninfo = pg_strdup(optarg);
				break;
			case 'r':
				opts->reconnect_seconds = atoi(optarg);
				break;
			case 's':
				opts->source_name = pg_strdup(optarg);
				break;
			case 'S':
				opts->stream_table = pg_strdup(optarg);
				break;
			case 'T':
				opts->topic_filter = pg_strdup(optarg);
				break;
			case 'V':
				puts("json_event_mqttd (PostgreSQL) " PG_VERSION);
				exit(0);
			case '?':
				usage(progname);
				exit(0);
			default:
				pg_fatal("try \"%s --help\" for more information", progname);
		}
	}

	if (opts->reconnect_seconds < 1)
		opts->reconnect_seconds = 1;
}

static void
load_source_config(PGconn *conn, MqttOptions *opts)
{
	PGresult   *res;
	const char *params[1];
	const char *sql =
		"SELECT stream_table::text, config->>'broker', config->>'topic' "
		"FROM json_event_sources "
		"WHERE source_name = $1 AND source_type = 'mqtt' AND enabled";

	if (opts->source_name == NULL)
		return;

	params[0] = opts->source_name;
	res = PQexecParams(conn, sql, 1, NULL, params, NULL, NULL, 0);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		pg_fatal("could not load MQTT source %s: %s",
				 opts->source_name, PQerrorMessage(conn));
	if (PQntuples(res) != 1)
		pg_fatal("enabled MQTT source %s was not found", opts->source_name);

	opts->stream_table = pg_strdup(PQgetvalue(res, 0, 0));
	opts->broker_uri = pg_strdup(PQgetvalue(res, 0, 1));
	opts->topic_filter = pg_strdup(PQgetvalue(res, 0, 2));
	PQclear(res);
}

static BrokerAddress
parse_broker_uri(const char *uri)
{
	BrokerAddress addr;
	const char *host_start;
	const char *host_end;
	const char *port_start;

	addr.host = NULL;
	addr.port = pg_strdup("1883");

	if (uri == NULL || strncmp(uri, "mqtt://", 7) != 0)
		pg_fatal("only plain mqtt:// broker URIs are supported");

	host_start = uri + 7;
	host_end = host_start;
	while (*host_end != '\0' && *host_end != ':' && *host_end != '/')
		host_end++;

	if (host_end == host_start)
		pg_fatal("broker URI has no host: %s", uri);

	addr.host = pnstrdup(host_start, host_end - host_start);
	if (*host_end == ':')
	{
		port_start = host_end + 1;
		host_end = port_start;
		while (*host_end != '\0' && *host_end != '/')
			host_end++;
		free(addr.port);
		addr.port = pnstrdup(port_start, host_end - port_start);
	}

	return addr;
}

static int
connect_tcp(const char *host, const char *port)
{
	struct addrinfo hints;
	struct addrinfo *result;
	struct addrinfo *rp;
	int			fd = -1;
	int			rc;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	rc = getaddrinfo(host, port, &hints, &result);
	if (rc != 0)
	{
		pg_log_error("could not resolve MQTT broker %s:%s: %s",
					 host, port, gai_strerror(rc));
		return -1;
	}

	for (rp = result; rp != NULL; rp = rp->ai_next)
	{
		fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (fd < 0)
			continue;
		if (connect(fd, rp->ai_addr, rp->ai_addrlen) == 0)
			break;
		close(fd);
		fd = -1;
	}

	freeaddrinfo(result);
	return fd;
}

static bool
mqtt_connect(int fd, const char *client_id)
{
	StringInfoData vh;
	StringInfoData pkt;
	unsigned char packet_type;
	unsigned char *payload = NULL;
	size_t		payload_len = 0;
	uint16		client_len = strlen(client_id);

	initStringInfo(&vh);
	appendStringInfoChar(&vh, 0);
	appendStringInfoChar(&vh, 4);
	appendBinaryStringInfo(&vh, "MQTT", 4);
	appendStringInfoChar(&vh, 4);	/* MQTT 3.1.1 */
	appendStringInfoChar(&vh, 2);	/* clean session */
	appendStringInfoChar(&vh, 0);
	appendStringInfoChar(&vh, DEFAULT_KEEPALIVE);
	appendStringInfoChar(&vh, client_len >> 8);
	appendStringInfoChar(&vh, client_len & 0xff);
	appendBinaryStringInfo(&vh, client_id, client_len);

	initStringInfo(&pkt);
	appendStringInfoChar(&pkt, 0x10);
	mqtt_write_remaining_length(&pkt, vh.len);
	appendBinaryStringInfo(&pkt, vh.data, vh.len);

	if (!mqtt_write_all(fd, (unsigned char *) pkt.data, pkt.len))
		return false;

	if (!mqtt_read_packet(fd, &packet_type, &payload, &payload_len))
		return false;
	if (packet_type != 0x20 || payload_len < 2 || payload[1] != 0)
	{
		free(payload);
		return false;
	}

	free(payload);
	return true;
}

static bool
mqtt_subscribe(int fd, const char *topic_filter)
{
	StringInfoData body;
	StringInfoData pkt;
	unsigned char packet_type;
	unsigned char *payload = NULL;
	size_t		payload_len = 0;
	uint16		topic_len = strlen(topic_filter);

	initStringInfo(&body);
	appendStringInfoChar(&body, 0);
	appendStringInfoChar(&body, 1);
	appendStringInfoChar(&body, topic_len >> 8);
	appendStringInfoChar(&body, topic_len & 0xff);
	appendBinaryStringInfo(&body, topic_filter, topic_len);
	appendStringInfoChar(&body, 0);	/* QoS 0 */

	initStringInfo(&pkt);
	appendStringInfoChar(&pkt, 0x82);
	mqtt_write_remaining_length(&pkt, body.len);
	appendBinaryStringInfo(&pkt, body.data, body.len);

	if (!mqtt_write_all(fd, (unsigned char *) pkt.data, pkt.len))
		return false;

	if (!mqtt_read_packet(fd, &packet_type, &payload, &payload_len))
		return false;
	if (packet_type != 0x90 || payload_len < 3 || payload[payload_len - 1] == 0x80)
	{
		free(payload);
		return false;
	}

	free(payload);
	return true;
}

static bool
mqtt_write_all(int fd, const unsigned char *buf, size_t len)
{
	size_t		written = 0;

	while (written < len)
	{
		ssize_t		rc = send(fd, buf + written, len - written, 0);

		if (rc <= 0)
			return false;
		written += rc;
	}

	return true;
}

static void
mqtt_write_remaining_length(StringInfo out, size_t len)
{
	do
	{
		unsigned char encoded = len % 128;

		len /= 128;
		if (len > 0)
			encoded |= 128;
		appendStringInfoChar(out, encoded);
	} while (len > 0);
}

static bool
mqtt_read_remaining_length(int fd, size_t *len)
{
	int			multiplier = 1;
	int			loops = 0;
	unsigned char encoded;

	*len = 0;
	do
	{
		if (recv(fd, &encoded, 1, 0) != 1)
			return false;
		*len += (encoded & 127) * multiplier;
		multiplier *= 128;
		loops++;
		if (loops > 4 || *len > MQTT_MAX_PACKET)
			return false;
	} while ((encoded & 128) != 0);

	return true;
}

static bool
mqtt_read_packet(int fd, unsigned char *packet_type,
				 unsigned char **packet, size_t *packet_len)
{
	unsigned char fixed;
	size_t		got = 0;

	if (recv(fd, &fixed, 1, 0) != 1)
		return false;
	*packet_type = fixed;

	if (!mqtt_read_remaining_length(fd, packet_len))
		return false;
	*packet = pg_malloc(*packet_len + 1);

	while (got < *packet_len)
	{
		ssize_t		rc = recv(fd, *packet + got, *packet_len - got, 0);

		if (rc <= 0)
		{
			free(*packet);
			*packet = NULL;
			return false;
		}
		got += rc;
	}

	(*packet)[*packet_len] = '\0';
	return true;
}

static bool
mqtt_handle_publish(int fd, PGconn *conn, const MqttOptions *opts,
					unsigned char packet_type,
					const unsigned char *packet, size_t packet_len)
{
	uint16		topic_len;
	char	   *topic;
	char	   *payload;
	char	   *error_message = NULL;
	size_t		pos = 2;
	int			qos = (packet_type & 0x06) >> 1;
	uint16		packet_id = 0;

	if ((packet_type & 0xf0) != 0x30)
		return false;
	if (packet_len < 2)
		return false;

	topic_len = ((uint16) packet[0] << 8) | packet[1];
	if (packet_len < 2 + topic_len)
		return false;
	topic = pnstrdup((const char *) packet + 2, topic_len);
	pos += topic_len;

	if (qos > 0)
	{
		if (packet_len < pos + 2)
		{
			free(topic);
			return false;
		}
		packet_id = ((uint16) packet[pos] << 8) | packet[pos + 1];
		pos += 2;
	}

	payload = pnstrdup((const char *) packet + pos, packet_len - pos);

	if (!sql_ingest(conn, opts, topic, payload, &error_message))
	{
		sql_reject(conn, opts, topic, payload, "ingest_failed",
				   error_message ? error_message : "ingest failed");
		sql_set_error(conn, opts->source_name, error_message);
		free(error_message);
	}

	if (qos == 1)
	{
		unsigned char puback[4];

		puback[0] = 0x40;
		puback[1] = 0x02;
		puback[2] = packet_id >> 8;
		puback[3] = packet_id & 0xff;
		(void) mqtt_write_all(fd, puback, sizeof(puback));
	}

	free(topic);
	free(payload);
	return opts->once;
}

static bool
sql_ingest(PGconn *conn, const MqttOptions *opts, const char *topic,
		   const char *payload, char **error_message)
{
	PGresult   *res;
	char	   *headers_json = build_headers_json(opts, topic);
	const char *source = opts->source_name ? opts->source_name : "mqtt";
	const char *params[5];
	const char *sql =
		"SELECT json_event_stream_ingest($1::regclass, $2, $3, $4::jsonb, $5::jsonb)";

	params[0] = opts->stream_table;
	params[1] = source;
	params[2] = topic;
	params[3] = payload;
	params[4] = headers_json;

	res = PQexecParams(conn, sql, 5, NULL, params, NULL, NULL, 0);
	free(headers_json);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		*error_message = pg_strdup(PQerrorMessage(conn));
		PQclear(res);
		return false;
	}

	PQclear(res);
	return true;
}

static void
sql_reject(PGconn *conn, const MqttOptions *opts, const char *topic,
		   const char *payload, const char *error_code,
		   const char *error_message)
{
	PGresult   *res;
	char	   *headers_json = build_headers_json(opts, topic);
	const char *source = opts->source_name ? opts->source_name : "mqtt";
	const char *params[7];
	const char *sql =
		"SELECT json_event_stream_reject($1::regclass, $2, $3, $4, $5, $6, $7::jsonb)";

	params[0] = opts->stream_table;
	params[1] = source;
	params[2] = topic;
	params[3] = payload ? payload : "";
	params[4] = error_code;
	params[5] = error_message ? error_message : "";
	params[6] = headers_json;

	res = PQexecParams(conn, sql, 7, NULL, params, NULL, NULL, 0);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		pg_log_warning("could not write MQTT dead letter: %s", PQerrorMessage(conn));
	PQclear(res);
	free(headers_json);
}

static void
sql_set_error(PGconn *conn, const char *source_name, const char *error_message)
{
	PGresult   *res;
	const char *params[2];

	if (source_name == NULL)
		return;

	params[0] = source_name;
	params[1] = error_message ? error_message : NULL;
	res = PQexecParams(conn,
					   "UPDATE json_event_sources SET last_error = $2 WHERE source_name = $1",
					   2, NULL, params, NULL, NULL, 0);
	PQclear(res);
}

static char *
json_escape(const char *input)
{
	StringInfoData buf;
	const unsigned char *ptr;

	initStringInfo(&buf);
	appendStringInfoChar(&buf, '"');
	for (ptr = (const unsigned char *) input; ptr != NULL && *ptr != '\0'; ptr++)
	{
		if (*ptr == '"' || *ptr == '\\')
			appendStringInfo(&buf, "\\%c", *ptr);
		else if (*ptr == '\n')
			appendStringInfoString(&buf, "\\n");
		else if (*ptr == '\r')
			appendStringInfoString(&buf, "\\r");
		else if (*ptr == '\t')
			appendStringInfoString(&buf, "\\t");
		else if (*ptr < 0x20)
			appendStringInfo(&buf, "\\u%04x", *ptr);
		else
			appendStringInfoChar(&buf, *ptr);
	}
	appendStringInfoChar(&buf, '"');
	return buf.data;
}

static char *
build_headers_json(const MqttOptions *opts, const char *topic)
{
	char	   *broker = json_escape(opts->broker_uri ? opts->broker_uri : "");
	char	   *filter = json_escape(opts->topic_filter ? opts->topic_filter : "");
	char	   *actual_topic = json_escape(topic ? topic : "");
	char	   *json;

	json = psprintf("{\"broker\":%s,\"topic_filter\":%s,\"mqtt_topic\":%s}",
					broker, filter, actual_topic);
	free(broker);
	free(filter);
	free(actual_topic);
	return json;
}

static void
signal_handler(SIGNAL_ARGS)
{
	shutdown_requested = 1;
}

int
main(int argc, char **argv)
{
	MqttOptions opts;
	PGconn	   *conn;

	pg_logging_init(argv[0]);
	parse_options(argc, argv, &opts);

	conn = PQconnectdb(opts.conninfo);
	if (PQstatus(conn) != CONNECTION_OK)
		pg_fatal("could not connect to database: %s", PQerrorMessage(conn));

	load_source_config(conn, &opts);

	if (opts.broker_uri == NULL || opts.topic_filter == NULL || opts.stream_table == NULL)
		pg_fatal("broker, topic, and stream are required unless --source supplies them");

	pqsignal(SIGTERM, signal_handler);
	pqsignal(SIGINT, signal_handler);

	while (!shutdown_requested)
	{
		BrokerAddress addr = parse_broker_uri(opts.broker_uri);
		int			fd;

		fd = connect_tcp(addr.host, addr.port);
		if (fd < 0)
		{
			sql_set_error(conn, opts.source_name, "could not connect to MQTT broker");
			pg_log_error("could not connect to MQTT broker %s:%s", addr.host, addr.port);
		}
		else if (!mqtt_connect(fd, opts.client_id))
		{
			sql_set_error(conn, opts.source_name, "MQTT CONNECT failed");
			pg_log_error("MQTT CONNECT failed");
			close(fd);
		}
		else if (!mqtt_subscribe(fd, opts.topic_filter))
		{
			sql_set_error(conn, opts.source_name, "MQTT SUBSCRIBE failed");
			pg_log_error("MQTT SUBSCRIBE failed");
			close(fd);
		}
		else
		{
			pg_log_info("subscribed to %s from %s", opts.topic_filter, opts.broker_uri);

			while (!shutdown_requested)
			{
				unsigned char packet_type;
				unsigned char *packet = NULL;
				size_t		packet_len = 0;
				bool		done;

				if (!mqtt_read_packet(fd, &packet_type, &packet, &packet_len))
					break;

				done = mqtt_handle_publish(fd, conn, &opts, packet_type, packet, packet_len);
				free(packet);

				if (done)
				{
					close(fd);
					PQfinish(conn);
					return 0;
				}
			}

			close(fd);
		}

		free(addr.host);
		free(addr.port);

		if (!shutdown_requested)
			sleep(opts.reconnect_seconds);
	}

	PQfinish(conn);
	return 0;
}
