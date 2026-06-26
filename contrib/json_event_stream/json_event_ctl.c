/*
 * json_event_ctl
 *
 * Native command layer for json_event_stream management.
 */
#include "postgres_fe.h"

#include "common/logging.h"
#include "common/string.h"
#include "getopt_long.h"
#include "libpq-fe.h"
#include "pg_getopt.h"

typedef struct
{
	char	   *conninfo;
	bool		echo;
} GlobalOptions;

static void usage(const char *progname);
static PGconn *connect_database(const char *conninfo);
static void run_command(PGconn *conn, const char *sql, int nparams,
						const char *const *params, bool echo);
static void run_query(PGconn *conn, const char *sql, int nparams,
					  const char *const *params, bool echo);
static void print_result(PGresult *res);
static void parse_global_options(int *argc, char ***argv, GlobalOptions *opts);
static void command_init(PGconn *conn, bool echo);
static void command_create_stream(PGconn *conn, int argc, char **argv, bool echo);
static void command_create_mqtt_source(PGconn *conn, int argc, char **argv, bool echo);
static void command_create_http_source(PGconn *conn, int argc, char **argv, bool echo);
static void command_set_retention(PGconn *conn, int argc, char **argv, bool echo);
static void command_status(PGconn *conn, int argc, char **argv, bool echo);

static void
usage(const char *progname)
{
	printf("%s manages JSON event streams.\n\n", progname);
	printf("Usage:\n");
	printf("  %s [GLOBAL_OPTION] init\n", progname);
	printf("  %s [GLOBAL_OPTION] create-stream STREAM [OPTION]...\n", progname);
	printf("  %s [GLOBAL_OPTION] create-mqtt-source NAME --broker URI --topic FILTER --stream STREAM\n", progname);
	printf("  %s [GLOBAL_OPTION] create-http-source NAME --route ROUTE --stream STREAM\n", progname);
	printf("  %s [GLOBAL_OPTION] set-retention STREAM INTERVAL\n", progname);
	printf("  %s [GLOBAL_OPTION] status\n\n", progname);
	printf("Global options:\n");
	printf("  -d, --dbname=CONNINFO      libpq connection string (default: dbname=postgres)\n");
	printf("  -e, --echo                 print SQL before executing it\n");
	printf("  -V, --version              output version information, then exit\n");
	printf("  -?, --help                 show this help, then exit\n\n");
	printf("create-stream options:\n");
	printf("  --schema=SCHEMA            target schema (default: current_schema())\n");
	printf("  --partition-interval=INT   partition interval (default: 1 day)\n");
	printf("  --retention=INT            retention interval\n");
	printf("  --payload-gin              create payload jsonb_path_ops GIN index\n");
	printf("\nSource options:\n");
	printf("  --disabled                 create source disabled\n");
}

static void
parse_global_options(int *argc, char ***argv, GlobalOptions *opts)
{
	static const struct option long_options[] = {
		{"dbname", required_argument, NULL, 'd'},
		{"echo", no_argument, NULL, 'e'},
		{"version", no_argument, NULL, 'V'},
		{"help", no_argument, NULL, '?'},
		{NULL, 0, NULL, 0}
	};
	int			c;

	opts->conninfo = pg_strdup("dbname=postgres");
	opts->echo = false;
	optind = 1;

	while ((c = getopt_long(*argc, *argv, "+d:eV?", long_options, NULL)) != -1)
	{
		switch (c)
		{
			case 'd':
				opts->conninfo = pg_strdup(optarg);
				break;
			case 'e':
				opts->echo = true;
				break;
			case 'V':
				puts("json_event_ctl (PostgreSQL) " PG_VERSION);
				exit(0);
			case '?':
				usage(get_progname((*argv)[0]));
				exit(0);
			default:
				pg_fatal("try \"%s --help\" for more information",
						 get_progname((*argv)[0]));
		}
	}

	*argc -= optind;
	*argv += optind;
}

static PGconn *
connect_database(const char *conninfo)
{
	PGconn	   *conn = PQconnectdb(conninfo);

	if (PQstatus(conn) != CONNECTION_OK)
		pg_fatal("could not connect to database: %s", PQerrorMessage(conn));
	return conn;
}

static void
run_command(PGconn *conn, const char *sql, int nparams,
			const char *const *params, bool echo)
{
	PGresult   *res;

	if (echo)
		printf("%s\n", sql);

	res = PQexecParams(conn, sql, nparams, NULL, params, NULL, NULL, 0);
	if (PQresultStatus(res) != PGRES_COMMAND_OK &&
		PQresultStatus(res) != PGRES_TUPLES_OK)
		pg_fatal("%s", PQerrorMessage(conn));

	if (PQresultStatus(res) == PGRES_TUPLES_OK)
		print_result(res);
	PQclear(res);
}

static void
run_query(PGconn *conn, const char *sql, int nparams,
		  const char *const *params, bool echo)
{
	PGresult   *res;

	if (echo)
		printf("%s\n", sql);

	res = PQexecParams(conn, sql, nparams, NULL, params, NULL, NULL, 0);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		pg_fatal("%s", PQerrorMessage(conn));

	print_result(res);
	PQclear(res);
}

static void
print_result(PGresult *res)
{
	int			i;
	int			j;

	for (j = 0; j < PQnfields(res); j++)
	{
		if (j > 0)
			putchar('\t');
		fputs(PQfname(res, j), stdout);
	}
	putchar('\n');

	for (i = 0; i < PQntuples(res); i++)
	{
		for (j = 0; j < PQnfields(res); j++)
		{
			if (j > 0)
				putchar('\t');
			if (!PQgetisnull(res, i, j))
				fputs(PQgetvalue(res, i, j), stdout);
		}
		putchar('\n');
	}
}

static void
command_init(PGconn *conn, bool echo)
{
	run_command(conn, "CREATE EXTENSION IF NOT EXISTS json_event_stream",
				0, NULL, echo);
}

static void
command_create_stream(PGconn *conn, int argc, char **argv, bool echo)
{
	static const struct option long_options[] = {
		{"schema", required_argument, NULL, 1},
		{"partition-interval", required_argument, NULL, 2},
		{"retention", required_argument, NULL, 3},
		{"payload-gin", no_argument, NULL, 4},
		{NULL, 0, NULL, 0}
	};
	char	   *schema = NULL;
	char	   *partition_interval = pg_strdup("1 day");
	char	   *retention = NULL;
	const char *payload_gin = "false";
	const char *params[5];
	int			c;

	optind = 1;
	while ((c = getopt_long(argc, argv, "", long_options, NULL)) != -1)
	{
		switch (c)
		{
			case 1:
				schema = pg_strdup(optarg);
				break;
			case 2:
				partition_interval = pg_strdup(optarg);
				break;
			case 3:
				retention = pg_strdup(optarg);
				break;
			case 4:
				payload_gin = "true";
				break;
			default:
				pg_fatal("invalid create-stream option");
		}
	}

	if (optind >= argc)
		pg_fatal("create-stream requires STREAM");

	if (schema == NULL)
	{
		const char *params4[4];

		params4[0] = argv[optind];
		params4[1] = partition_interval;
		params4[2] = retention;
		params4[3] = payload_gin;
		run_query(conn,
				  "SELECT json_event_stream_create($1::name, $2::interval, $3::interval, $4::boolean) AS stream",
				  4, params4, echo);
		return;
	}

	params[0] = schema;
	params[1] = argv[optind];
	params[2] = partition_interval;
	params[3] = retention;
	params[4] = payload_gin;
	run_query(conn,
			  "SELECT json_event_stream_create($1::name, $2::name, $3::interval, $4::interval, $5::boolean) AS stream",
			  5, params, echo);
}

static void
command_create_mqtt_source(PGconn *conn, int argc, char **argv, bool echo)
{
	static const struct option long_options[] = {
		{"broker", required_argument, NULL, 1},
		{"topic", required_argument, NULL, 2},
		{"stream", required_argument, NULL, 3},
		{"disabled", no_argument, NULL, 4},
		{NULL, 0, NULL, 0}
	};
	char	   *broker = NULL;
	char	   *topic = NULL;
	char	   *stream = NULL;
	const char *enabled = "true";
	const char *params[5];
	int			c;

	optind = 1;
	while ((c = getopt_long(argc, argv, "", long_options, NULL)) != -1)
	{
		if (c == 1)
			broker = pg_strdup(optarg);
		else if (c == 2)
			topic = pg_strdup(optarg);
		else if (c == 3)
			stream = pg_strdup(optarg);
		else if (c == 4)
			enabled = "false";
		else
			pg_fatal("invalid create-mqtt-source option");
	}

	if (optind >= argc || broker == NULL || topic == NULL || stream == NULL)
		pg_fatal("create-mqtt-source requires NAME, --broker, --topic, and --stream");

	params[0] = argv[optind];
	params[1] = broker;
	params[2] = topic;
	params[3] = stream;
	params[4] = enabled;
	run_query(conn,
			  "SELECT json_event_mqtt_source_create($1::name, $2, $3, $4::regclass, '{}'::jsonb, $5::boolean) AS source",
			  5, params, echo);
}

static void
command_create_http_source(PGconn *conn, int argc, char **argv, bool echo)
{
	static const struct option long_options[] = {
		{"route", required_argument, NULL, 1},
		{"stream", required_argument, NULL, 2},
		{"disabled", no_argument, NULL, 3},
		{NULL, 0, NULL, 0}
	};
	char	   *route = NULL;
	char	   *stream = NULL;
	const char *enabled = "true";
	const char *params[4];
	int			c;

	optind = 1;
	while ((c = getopt_long(argc, argv, "", long_options, NULL)) != -1)
	{
		if (c == 1)
			route = pg_strdup(optarg);
		else if (c == 2)
			stream = pg_strdup(optarg);
		else if (c == 3)
			enabled = "false";
		else
			pg_fatal("invalid create-http-source option");
	}

	if (optind >= argc || route == NULL || stream == NULL)
		pg_fatal("create-http-source requires NAME, --route, and --stream");

	params[0] = argv[optind];
	params[1] = route;
	params[2] = stream;
	params[3] = enabled;
	run_query(conn,
			  "SELECT json_event_http_source_create($1::name, $2, $3::regclass, '{}'::jsonb, $4::boolean) AS source",
			  4, params, echo);
}

static void
command_set_retention(PGconn *conn, int argc, char **argv, bool echo)
{
	const char *params[2];

	if (argc != 3)
		pg_fatal("set-retention requires STREAM and INTERVAL");

	params[0] = argv[1];
	params[1] = argv[2];
	run_command(conn,
				"SELECT json_event_stream_set_retention($1::regclass, $2::interval)",
				2, params, echo);
}

static void
command_status(PGconn *conn, int argc, char **argv, bool echo)
{
	if (argc != 1)
		pg_fatal("status does not accept arguments");

	run_query(conn,
			  "SELECT s.stream_table::text AS stream, s.partition_interval::text, "
			  "COALESCE(s.retention::text, '') AS retention, "
			  "count(DISTINCT p.partition_table) AS partitions, "
			  "count(DISTINCT src.source_name) AS sources "
			  "FROM json_event_streams s "
			  "LEFT JOIN json_event_partition_status p ON p.stream_table = s.stream_table "
			  "LEFT JOIN json_event_sources src ON src.stream_table = s.stream_table "
			  "GROUP BY s.stream_table, s.partition_interval, s.retention "
			  "ORDER BY s.stream_table::text",
			  0, NULL, echo);
}

int
main(int argc, char **argv)
{
	GlobalOptions opts;
	PGconn	   *conn;
	char	   *command;
	const char *progname;

	pg_logging_init(argv[0]);
	progname = get_progname(argv[0]);
	parse_global_options(&argc, &argv, &opts);

	if (argc < 1)
	{
		usage(progname);
		exit(1);
	}

	command = argv[0];
	conn = connect_database(opts.conninfo);

	if (strcmp(command, "init") == 0)
		command_init(conn, opts.echo);
	else if (strcmp(command, "create-stream") == 0)
		command_create_stream(conn, argc, argv, opts.echo);
	else if (strcmp(command, "create-mqtt-source") == 0)
		command_create_mqtt_source(conn, argc, argv, opts.echo);
	else if (strcmp(command, "create-http-source") == 0)
		command_create_http_source(conn, argc, argv, opts.echo);
	else if (strcmp(command, "set-retention") == 0)
		command_set_retention(conn, argc, argv, opts.echo);
	else if (strcmp(command, "status") == 0)
		command_status(conn, argc, argv, opts.echo);
	else
		pg_fatal("unknown command: %s", command);

	PQfinish(conn);
	return 0;
}
