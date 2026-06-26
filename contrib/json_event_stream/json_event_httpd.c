/*
 * json_event_httpd
 *
 * Small libpq-backed HTTP ingestion daemon for json_event_stream.
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

#define DEFAULT_LISTEN_ADDR "127.0.0.1"
#define DEFAULT_LISTEN_PORT "8080"
#define DEFAULT_MAX_BODY (1024 * 1024)
#define HEADER_LIMIT 32768

typedef struct
{
	char	   *conninfo;
	char	   *listen_addr;
	char	   *listen_port;
	char	   *auth_token;
	size_t		max_body;
} HttpdOptions;

typedef struct
{
	char		method[16];
	char		path[1024];
	char	   *body;
	size_t		body_len;
	char	   *content_type;
	char	   *authorization;
	char	   *source;
	char	   *topic;
} HttpRequest;

static volatile sig_atomic_t shutdown_requested = 0;

static void usage(const char *progname);
static void parse_options(int argc, char **argv, HttpdOptions *opts);
static int	start_listener(const char *addr, const char *port);
static void serve_forever(int server_fd, PGconn *conn, const HttpdOptions *opts);
static void handle_client(int client_fd, PGconn *conn, const HttpdOptions *opts);
static bool read_request(int client_fd, const HttpdOptions *opts, HttpRequest *req,
						 char **error_message, int *status_code);
static void free_request(HttpRequest *req);
static char *get_header_value(const char *headers, const char *name);
static bool route_stream(const char *path, bool *batch, char **stream_name);
static char *url_decode_segment(const char *start, size_t len);
static char *json_escape(const char *input);
static char *build_headers_json(const HttpRequest *req);
static bool sql_ingest(PGconn *conn, const char *stream_name, const HttpRequest *req,
					   bool batch, char **error_message);
static void sql_reject(PGconn *conn, const char *stream_name, const HttpRequest *req,
					   const char *error_code, const char *error_message);
static void send_response(int client_fd, int status, const char *status_text,
						  const char *body);
static void signal_handler(SIGNAL_ARGS);

static void
usage(const char *progname)
{
	printf("%s accepts JSON HTTP events into json_event_stream streams.\n\n", progname);
	printf("Usage:\n");
	printf("  %s [OPTION]...\n\n", progname);
	printf("Options:\n");
	printf("  -d, --dbname=CONNINFO      libpq connection string (default: dbname=postgres)\n");
	printf("  -l, --listen=ADDRESS       listen address (default: %s)\n", DEFAULT_LISTEN_ADDR);
	printf("  -p, --port=PORT            listen port (default: %s)\n", DEFAULT_LISTEN_PORT);
	printf("  -m, --max-body=BYTES       maximum request body (default: %d)\n", DEFAULT_MAX_BODY);
	printf("  -t, --token=TOKEN          require Authorization: Bearer TOKEN\n");
	printf("  -V, --version              output version information, then exit\n");
	printf("  -?, --help                 show this help, then exit\n\n");
	printf("Routes:\n");
	printf("  POST /streams/STREAM/events       ingest one JSON payload\n");
	printf("  POST /streams/STREAM/events/batch ingest a JSON array of payloads\n");
	printf("\nHeaders:\n");
	printf("  X-Json-Event-Source overrides source (default: http)\n");
	printf("  X-Json-Event-Topic  overrides topic  (default: request path)\n");
}

static void
parse_options(int argc, char **argv, HttpdOptions *opts)
{
	static const struct option long_options[] = {
		{"dbname", required_argument, NULL, 'd'},
		{"listen", required_argument, NULL, 'l'},
		{"max-body", required_argument, NULL, 'm'},
		{"port", required_argument, NULL, 'p'},
		{"token", required_argument, NULL, 't'},
		{"version", no_argument, NULL, 'V'},
		{"help", no_argument, NULL, '?'},
		{NULL, 0, NULL, 0}
	};
	int			c;
	const char *progname;

	progname = get_progname(argv[0]);
	opts->conninfo = pg_strdup("dbname=postgres");
	opts->listen_addr = pg_strdup(DEFAULT_LISTEN_ADDR);
	opts->listen_port = pg_strdup(DEFAULT_LISTEN_PORT);
	opts->auth_token = NULL;
	opts->max_body = DEFAULT_MAX_BODY;

	while ((c = getopt_long(argc, argv, "d:l:m:p:t:V?", long_options, NULL)) != -1)
	{
		switch (c)
		{
			case 'd':
				opts->conninfo = pg_strdup(optarg);
				break;
			case 'l':
				opts->listen_addr = pg_strdup(optarg);
				break;
			case 'm':
				opts->max_body = strtoul(optarg, NULL, 10);
				break;
			case 'p':
				opts->listen_port = pg_strdup(optarg);
				break;
			case 't':
				opts->auth_token = pg_strdup(optarg);
				break;
			case 'V':
				puts("json_event_httpd (PostgreSQL) " PG_VERSION);
				exit(0);
			case '?':
				usage(progname);
				exit(0);
			default:
				pg_fatal("try \"%s --help\" for more information", progname);
		}
	}
}

static int
start_listener(const char *addr, const char *port)
{
	struct addrinfo hints;
	struct addrinfo *result;
	struct addrinfo *rp;
	int			server_fd = -1;
	int			rc;
	int			one = 1;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	rc = getaddrinfo(addr, port, &hints, &result);
	if (rc != 0)
		pg_fatal("could not resolve listen address %s:%s: %s",
				 addr, port, gai_strerror(rc));

	for (rp = result; rp != NULL; rp = rp->ai_next)
	{
		server_fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (server_fd < 0)
			continue;

		(void) setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

		if (bind(server_fd, rp->ai_addr, rp->ai_addrlen) == 0 &&
			listen(server_fd, 64) == 0)
			break;

		close(server_fd);
		server_fd = -1;
	}

	freeaddrinfo(result);

	if (server_fd < 0)
		pg_fatal("could not listen on %s:%s: %m", addr, port);

	return server_fd;
}

static void
serve_forever(int server_fd, PGconn *conn, const HttpdOptions *opts)
{
	while (!shutdown_requested)
	{
		int			client_fd;

		client_fd = accept(server_fd, NULL, NULL);
		if (client_fd < 0)
		{
			if (errno == EINTR)
				continue;
			pg_log_error("accept failed: %m");
			continue;
		}

		handle_client(client_fd, conn, opts);
		close(client_fd);
	}
}

static void
handle_client(int client_fd, PGconn *conn, const HttpdOptions *opts)
{
	HttpRequest req;
	char	   *error_message = NULL;
	char	   *stream_name = NULL;
	bool		batch = false;
	int			status_code = 400;

	memset(&req, 0, sizeof(req));

	if (!read_request(client_fd, opts, &req, &error_message, &status_code))
	{
		if (req.path[0] != '\0' && route_stream(req.path, &batch, &stream_name))
			sql_reject(conn, stream_name, &req,
					   status_code == 401 ? "auth_failed" : "bad_request",
					   error_message ? error_message : "request could not be read");
		send_response(client_fd, status_code, "Bad Request",
					  error_message ? error_message : "{\"error\":\"bad request\"}");
		free(stream_name);
		free(error_message);
		free_request(&req);
		return;
	}

	if (!route_stream(req.path, &batch, &stream_name))
	{
		send_response(client_fd, 404, "Not Found", "{\"error\":\"unknown route\"}");
		free_request(&req);
		return;
	}

	if (!sql_ingest(conn, stream_name, &req, batch, &error_message))
	{
		sql_reject(conn, stream_name, &req, "ingest_failed",
				   error_message ? error_message : "ingest failed");
		send_response(client_fd, 400, "Bad Request",
					  error_message ? error_message : "{\"error\":\"ingest failed\"}");
		free(error_message);
		free(stream_name);
		free_request(&req);
		return;
	}

	send_response(client_fd, 202, "Accepted", "{\"status\":\"accepted\"}");
	free(stream_name);
	free_request(&req);
}

static bool
read_request(int client_fd, const HttpdOptions *opts, HttpRequest *req,
			 char **error_message, int *status_code)
{
	char		header_buf[HEADER_LIMIT + 1];
	size_t		header_len = 0;
	char	   *headers_end;
	char	   *content_length_header;
	long		content_length;
	char	   *line_end;
	char	   *space1;
	char	   *space2;
	ssize_t		nread;

	while (header_len < HEADER_LIMIT)
	{
		nread = recv(client_fd, header_buf + header_len, 1, 0);
		if (nread <= 0)
			break;
		header_len += nread;
		header_buf[header_len] = '\0';
		if (strstr(header_buf, "\r\n\r\n") != NULL)
			break;
	}

	headers_end = strstr(header_buf, "\r\n\r\n");
	if (headers_end == NULL)
	{
		*error_message = pg_strdup("{\"error\":\"headers too large or incomplete\"}");
		*status_code = 431;
		return false;
	}

	line_end = strstr(header_buf, "\r\n");
	if (line_end == NULL)
	{
		*error_message = pg_strdup("{\"error\":\"missing request line\"}");
		return false;
	}

	*line_end = '\0';
	space1 = strchr(header_buf, ' ');
	if (space1 == NULL)
	{
		*error_message = pg_strdup("{\"error\":\"malformed request line\"}");
		return false;
	}
	*space1 = '\0';
	space2 = strchr(space1 + 1, ' ');
	if (space2 == NULL)
	{
		*error_message = pg_strdup("{\"error\":\"malformed request line\"}");
		return false;
	}
	*space2 = '\0';

	strlcpy(req->method, header_buf, sizeof(req->method));
	strlcpy(req->path, space1 + 1, sizeof(req->path));
	*line_end = '\r';

	if (pg_strcasecmp(req->method, "POST") != 0)
	{
		*error_message = pg_strdup("{\"error\":\"method not allowed\"}");
		*status_code = 405;
		return false;
	}

	content_length_header = get_header_value(line_end + 2, "Content-Length");
	if (content_length_header == NULL)
	{
		*error_message = pg_strdup("{\"error\":\"missing content-length\"}");
		return false;
	}

	content_length = strtol(content_length_header, NULL, 10);
	free(content_length_header);
	if (content_length < 0)
	{
		*error_message = pg_strdup("{\"error\":\"invalid content-length\"}");
		return false;
	}
	if ((size_t) content_length > opts->max_body)
	{
		*error_message = pg_strdup("{\"error\":\"request body too large\"}");
		*status_code = 413;
		return false;
	}

	req->content_type = get_header_value(line_end + 2, "Content-Type");
	req->authorization = get_header_value(line_end + 2, "Authorization");
	req->source = get_header_value(line_end + 2, "X-Json-Event-Source");
	req->topic = get_header_value(line_end + 2, "X-Json-Event-Topic");

	if (opts->auth_token != NULL)
	{
		char	   *expected = psprintf("Bearer %s", opts->auth_token);
		bool		ok = req->authorization != NULL &&
			strcmp(req->authorization, expected) == 0;

		free(expected);
		if (!ok)
		{
			*error_message = pg_strdup("{\"error\":\"unauthorized\"}");
			*status_code = 401;
			return false;
		}
	}

	req->body = pg_malloc((size_t) content_length + 1);
	req->body_len = (size_t) content_length;

	{
		size_t		prefix_len = header_len - (headers_end + 4 - header_buf);
		size_t		copied = 0;

		if (prefix_len > req->body_len)
			prefix_len = req->body_len;
		memcpy(req->body, headers_end + 4, prefix_len);
		copied = prefix_len;

		while (copied < req->body_len)
		{
			nread = recv(client_fd, req->body + copied, req->body_len - copied, 0);
			if (nread <= 0)
				break;
			copied += nread;
		}

		if (copied != req->body_len)
		{
			*error_message = pg_strdup("{\"error\":\"incomplete request body\"}");
			return false;
		}
	}

	req->body[req->body_len] = '\0';
	return true;
}

static void
free_request(HttpRequest *req)
{
	free(req->body);
	free(req->content_type);
	free(req->authorization);
	free(req->source);
	free(req->topic);
}

static char *
get_header_value(const char *headers, const char *name)
{
	const char *line = headers;
	size_t		name_len = strlen(name);

	while (line != NULL && *line != '\0')
	{
		const char *line_end = strstr(line, "\r\n");
		const char *value_start;
		size_t		line_len;

		if (line_end == NULL)
			break;
		line_len = line_end - line;
		if (line_len == 0)
			break;

		if (line_len > name_len && line[name_len] == ':' &&
			pg_strncasecmp(line, name, name_len) == 0)
		{
			value_start = line + name_len + 1;
			while (*value_start == ' ' || *value_start == '\t')
				value_start++;
			while (line_end > value_start &&
				   (line_end[-1] == ' ' || line_end[-1] == '\t'))
				line_end--;
			return pnstrdup(value_start, line_end - value_start);
		}

		line = line_end + 2;
	}

	return NULL;
}

static bool
route_stream(const char *path, bool *batch, char **stream_name)
{
	const char *prefix = "/streams/";
	const char *suffix_single = "/events";
	const char *suffix_batch = "/events/batch";
	const char *stream_start;
	const char *suffix;
	size_t		stream_len;

	if (strncmp(path, prefix, strlen(prefix)) != 0)
		return false;

	stream_start = path + strlen(prefix);
	suffix = strstr(stream_start, suffix_batch);
	if (suffix != NULL && suffix[strlen(suffix_batch)] == '\0')
	{
		*batch = true;
		stream_len = suffix - stream_start;
	}
	else
	{
		suffix = strstr(stream_start, suffix_single);
		if (suffix == NULL || suffix[strlen(suffix_single)] != '\0')
			return false;
		*batch = false;
		stream_len = suffix - stream_start;
	}

	if (stream_len == 0)
		return false;

	*stream_name = url_decode_segment(stream_start, stream_len);
	return true;
}

static char *
url_decode_segment(const char *start, size_t len)
{
	char	   *out = pg_malloc(len + 1);
	size_t		i;
	size_t		j = 0;

	for (i = 0; i < len; i++)
	{
		if (start[i] == '%' && i + 2 < len &&
			isxdigit((unsigned char) start[i + 1]) &&
			isxdigit((unsigned char) start[i + 2]))
		{
			char		hex[3];

			hex[0] = start[i + 1];
			hex[1] = start[i + 2];
			hex[2] = '\0';
			out[j++] = (char) strtol(hex, NULL, 16);
			i += 2;
		}
		else
			out[j++] = start[i];
	}

	out[j] = '\0';
	return out;
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
build_headers_json(const HttpRequest *req)
{
	char	   *content_type = json_escape(req->content_type ? req->content_type : "");
	char	   *http_path = json_escape(req->path);
	char	   *json;

	json = psprintf("{\"content_type\":%s,\"http_path\":%s}",
					content_type, http_path);
	free(content_type);
	free(http_path);
	return json;
}

static bool
sql_ingest(PGconn *conn, const char *stream_name, const HttpRequest *req,
		   bool batch, char **error_message)
{
	PGresult   *res;
	char	   *headers_json;
	const char *source = req->source ? req->source : "http";
	const char *topic = req->topic ? req->topic : req->path;
	const char *params[5];
	const char *sql_single =
		"SELECT json_event_stream_ingest($1::regclass, $2, $3, $4::jsonb, $5::jsonb)";
	const char *sql_batch =
		"SELECT count(*) "
		"FROM jsonb_array_elements($4::jsonb) AS payload(doc), "
		"LATERAL json_event_stream_ingest($1::regclass, $2, $3, payload.doc, $5::jsonb)";

	headers_json = build_headers_json(req);
	params[0] = stream_name;
	params[1] = source;
	params[2] = topic;
	params[3] = req->body;
	params[4] = headers_json;

	res = PQexecParams(conn, batch ? sql_batch : sql_single,
					   5, NULL, params, NULL, NULL, 0);
	free(headers_json);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		*error_message = psprintf("{\"error\":%s}",
								  json_escape(PQerrorMessage(conn)));
		PQclear(res);
		return false;
	}

	PQclear(res);
	return true;
}

static void
sql_reject(PGconn *conn, const char *stream_name, const HttpRequest *req,
		   const char *error_code, const char *error_message)
{
	PGresult   *res;
	char	   *headers_json;
	const char *source = req->source ? req->source : "http";
	const char *topic = req->topic ? req->topic : req->path;
	const char *body = req->body ? req->body : "";
	const char *params[7];
	const char *sql =
		"SELECT json_event_stream_reject($1::regclass, $2, $3, $4, $5, $6, $7::jsonb)";

	headers_json = build_headers_json(req);
	params[0] = stream_name;
	params[1] = source;
	params[2] = topic;
	params[3] = body;
	params[4] = error_code;
	params[5] = error_message;
	params[6] = headers_json;

	res = PQexecParams(conn, sql, 7, NULL, params, NULL, NULL, 0);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		pg_log_warning("could not write dead letter: %s", PQerrorMessage(conn));

	PQclear(res);
	free(headers_json);
}

static void
send_response(int client_fd, int status, const char *status_text, const char *body)
{
	char	   *response;

	response = psprintf("HTTP/1.1 %d %s\r\n"
						"Content-Type: application/json\r\n"
						"Content-Length: %zu\r\n"
						"Connection: close\r\n"
						"\r\n"
						"%s",
						status, status_text, strlen(body), body);
	(void) send(client_fd, response, strlen(response), 0);
	free(response);
}

static void
signal_handler(SIGNAL_ARGS)
{
	shutdown_requested = 1;
}

int
main(int argc, char **argv)
{
	HttpdOptions opts;
	PGconn	   *conn;
	int			server_fd;

	pg_logging_init(argv[0]);
	parse_options(argc, argv, &opts);

	conn = PQconnectdb(opts.conninfo);
	if (PQstatus(conn) != CONNECTION_OK)
		pg_fatal("could not connect to database: %s", PQerrorMessage(conn));

	pqsignal(SIGTERM, signal_handler);
	pqsignal(SIGINT, signal_handler);

	server_fd = start_listener(opts.listen_addr, opts.listen_port);
	pg_log_info("listening on http://%s:%s", opts.listen_addr, opts.listen_port);
	serve_forever(server_fd, conn, &opts);

	close(server_fd);
	PQfinish(conn);
	return 0;
}
