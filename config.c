#include <ctype.h>
#include <getopt.h>

#include "config.h"
#include "mediator.h"

unsigned int config_interval = MEDIATOR_DEFAULT_INTERVAL;
uint8_t config_dumpcdr = MEDIATOR_DEFAULT_DUMPCDR;
uint8_t config_daemonize = MEDIATOR_DEFAULT_DAEMONIZE;
char *config_pid_path;

char *config_med_host;
char *config_med_user;
char *config_med_pass;
char *config_med_db;
unsigned int config_med_port = MEDIATOR_DEFAULT_ACCPORT;

char *config_cdr_host;
char *config_cdr_user;
char *config_cdr_pass;
char *config_cdr_db;
unsigned int config_cdr_port = MEDIATOR_DEFAULT_CDRPORT;

char *config_prov_host;
char *config_prov_user;
char *config_prov_pass;
char *config_prov_db;
unsigned int config_prov_port = MEDIATOR_DEFAULT_PROVPORT;

char *config_stats_host;
char *config_stats_user;
char *config_stats_pass;
char *config_stats_db;
unsigned int config_stats_port = MEDIATOR_DEFAULT_STATSPORT;
med_stats_period_t config_stats_period = MEDIATOR_DEFAULT_STATSPERIOD;

int config_maintenance = 0;
int strict_leg_tokens = 0;

enum config_option {
	OPT_DAEMONIZE = 'd',
	OPT_SYSLOG = 'l',
	OPT_PIDFILE = 'D',
	OPT_INTERVAL = 'i',
	OPT_MED_HOST = 'h',
	OPT_MED_USER = 'u',
	OPT_MED_PASS = 'p',
	OPT_MED_DB = 'b',
	OPT_MED_PORT = 'o',
	OPT_CDR_HOST = 'H',
	OPT_CDR_USER = 'U',
	OPT_CDR_PASS = 'P',
	OPT_CDR_DB = 'B',
	OPT_CDR_PORT = 'O',
	OPT_PROV_HOST = 'S',
	OPT_PROV_USER = 'R',
	OPT_PROV_PASS = 'A',
	OPT_PROV_DB = 'N',
	OPT_PROV_PORT = 'T',
	OPT_STATS_HOST = 'Z',
	OPT_STATS_USER = 'W',
	OPT_STATS_PASS = 'w',
	OPT_STATS_PORT = 'z',
	OPT_STATS_DB = 'X',
	OPT_STATS_PERIOD = 'x',
	OPT_MAINTENANCE = 'm',
	OPT_LEG_TOKENS = 's',
};

static const char options[] = "D:i:dl?h:u:p:b:o:H:U:P:B:O:S:T:R:A:N:Z:z:W:w:X:x:ms";

struct option long_options[] = {
	{ "pidfile", required_argument, NULL, OPT_PIDFILE },
	{ "daemonize", no_argument, NULL, OPT_DAEMONIZE },
	{ "syslog", optional_argument, NULL, OPT_SYSLOG },
	{ "interval", required_argument, NULL, OPT_INTERVAL },
	{ "med-host", required_argument, NULL, OPT_MED_HOST },
	{ "med-user", required_argument, NULL, OPT_MED_USER },
	{ "med-pass", required_argument, NULL, OPT_MED_PASS },
	{ "med-db", required_argument, NULL, OPT_MED_DB },
	{ "med-port", required_argument, NULL, OPT_MED_PORT },
	{ "cdr-host", required_argument, NULL, OPT_CDR_HOST },
	{ "cdr-user", required_argument, NULL, OPT_CDR_USER },
	{ "cdr-pass", required_argument, NULL, OPT_CDR_PASS },
	{ "cdr-db", required_argument, NULL, OPT_CDR_DB },
	{ "cdr-port", required_argument, NULL, OPT_CDR_PORT },
	{ "prov-host", required_argument, NULL, OPT_PROV_HOST },
	{ "prov-user", required_argument, NULL, OPT_PROV_USER },
	{ "prov-pass", required_argument, NULL, OPT_PROV_PASS },
	{ "prov-db", required_argument, NULL, OPT_PROV_DB },
	{ "prov-port", required_argument, NULL, OPT_PROV_PORT },
	{ "stats-host", required_argument, NULL, OPT_STATS_HOST },
	{ "stats-user", required_argument, NULL, OPT_STATS_USER },
	{ "stats-pass", required_argument, NULL, OPT_STATS_PASS },
	{ "stats-db", required_argument, NULL, OPT_STATS_DB },
	{ "stats-port", required_argument, NULL, OPT_STATS_PORT },
	{ "stats-period", required_argument, NULL, OPT_STATS_PERIOD },
	{ "maintenance", no_argument, NULL, OPT_MAINTENANCE },
	{ "leg-tokens", no_argument, NULL, OPT_LEG_TOKENS },
	{ NULL, 0, NULL, 0 },
};

static void config_help(const char *self, int rc)
{
	printf("mediator %s - Creates call detail records from OpenSER accounting.\n" \
		"Usage: %s [-?] [-d] [-D pidpath]\n" \
		"  -D\tThe path of the PID file (default = '%s').\n" \
		"  -d\tEnables daemonization of the process (default = disabled).\n" \
		"  -l\tEnables dumping of CDRs to syslog (default = disabled).\n" \
		"  -i\tThe creation interval (default = %d).\n" \
		"  -h\tThe ACC db host (default = '%s').\n" \
		"  -o\tThe ACC db port (default = '%d').\n" \
		"  -u\tThe ACC db user (default = '%s').\n" \
		"  -p\tThe ACC db pass (default = '%s').\n" \
		"  -b\tThe ACC db name (default = '%s').\n" \
		"  -H\tThe CDR db host (default = '%s').\n" \
		"  -O\tThe CDR db port (default = '%d').\n" \
		"  -U\tThe CDR db user (default = '%s').\n" \
		"  -P\tThe CDR db pass (default = '%s').\n" \
		"  -B\tThe CDR db name (default = '%s').\n" \
		"  -S\tThe prov db host (default = '%s').\n" \
		"  -T\tThe prov db port (default = '%d').\n" \
		"  -R\tThe prov db user (default = '%s').\n" \
		"  -A\tThe prov db pass (default = '%s').\n" \
		"  -N\tThe prov db name (default = '%s').\n" \
		"  -Z\tThe stats db host (default = '%s').\n" \
		"  -z\tThe stats db port (default = '%d').\n" \
		"  -W\tThe stats db user (default = '%s').\n" \
		"  -w\tThe stats db pass (default = '%s').\n" \
		"  -X\tThe stats db name (default = '%s').\n" \
		"  -x\tThe stats db period (default = '%d', 1=hour, 2=day, 3=month).\n" \
		"  -m\tMaintenance mode (do nothing, just sleep).\n" \
		"  -s\tStrict acc fields (move to trash otherwise).\n" \
		"  -?\tDisplays this message.\n",
		MEDIATOR_VERSION, self,
		MEDIATOR_DEFAULT_PIDPATH, MEDIATOR_DEFAULT_INTERVAL,
		MEDIATOR_DEFAULT_ACCHOST, MEDIATOR_DEFAULT_ACCPORT,
		MEDIATOR_DEFAULT_ACCUSER, MEDIATOR_DEFAULT_ACCPASS,
		MEDIATOR_DEFAULT_ACCDB,
		MEDIATOR_DEFAULT_CDRHOST, MEDIATOR_DEFAULT_CDRPORT,
		MEDIATOR_DEFAULT_CDRUSER, MEDIATOR_DEFAULT_CDRPASS,
		MEDIATOR_DEFAULT_CDRDB,
		MEDIATOR_DEFAULT_PROVHOST, MEDIATOR_DEFAULT_PROVPORT,
		MEDIATOR_DEFAULT_PROVUSER, MEDIATOR_DEFAULT_PROVPASS,
		MEDIATOR_DEFAULT_PROVDB,
		MEDIATOR_DEFAULT_STATSHOST, MEDIATOR_DEFAULT_STATSPORT,
		MEDIATOR_DEFAULT_STATSUSER, MEDIATOR_DEFAULT_STATSPASS,
		MEDIATOR_DEFAULT_STATSDB, MEDIATOR_DEFAULT_STATSPERIOD);

	exit(rc);
}

static void config_set_string_option(char **str, const char *opt)
{
	free(*str);
	*str = strdup(opt);
}

static void config_set_string_default(char **str, const char *def)
{
	if (*str == NULL)
		*str = strdup(def);
}

static void config_set_option(enum config_option option, const char *value)
{
	switch (option) {
	case OPT_DAEMONIZE:
		config_daemonize = 1;
		break;
	case OPT_SYSLOG:
		config_dumpcdr = 1;
		break;
	case OPT_PIDFILE:
		config_set_string_option(&config_pid_path, value);
		break;
	case OPT_INTERVAL:
		config_interval = atoi(value);
		break;
	case OPT_MED_HOST:
		config_set_string_option(&config_med_host, value);
		break;
	case OPT_MED_USER:
		config_set_string_option(&config_med_user, value);
		break;
	case OPT_MED_PASS:
		config_set_string_option(&config_med_pass, value);
		break;
	case OPT_MED_DB:
		config_set_string_option(&config_med_db, value);
		break;
	case OPT_MED_PORT:
		config_med_port = atoi(value);
		break;
	case OPT_CDR_HOST:
		config_set_string_option(&config_cdr_host, value);
		break;
	case OPT_CDR_USER:
		config_set_string_option(&config_cdr_user, value);
		break;
	case OPT_CDR_PASS:
		config_set_string_option(&config_cdr_pass, value);
		break;
	case OPT_CDR_DB:
		config_set_string_option(&config_cdr_db, value);
		break;
	case OPT_CDR_PORT:
		config_cdr_port = atoi(value);
		break;
	case OPT_PROV_HOST:
		config_set_string_option(&config_prov_host, value);
		break;
	case OPT_PROV_USER:
		config_set_string_option(&config_prov_user, value);
		break;
	case OPT_PROV_PASS:
		config_set_string_option(&config_prov_pass, value);
		break;
	case OPT_PROV_DB:
		config_set_string_option(&config_prov_db, value);
		break;
	case OPT_PROV_PORT:
		config_prov_port = atoi(value);
		break;
	case OPT_STATS_HOST:
		config_set_string_option(&config_stats_host, value);
		break;
	case OPT_STATS_USER:
		config_set_string_option(&config_stats_user, value);
		break;
	case OPT_STATS_PASS:
		config_set_string_option(&config_stats_pass, value);
		break;
	case OPT_STATS_PORT:
		config_stats_port = atoi(value);
		break;
	case OPT_STATS_DB:
		config_set_string_option(&config_stats_db, value);
		break;
	case OPT_STATS_PERIOD:
		config_stats_period = (med_stats_period_t)atoi(value);
		break;
	case OPT_MAINTENANCE:
		config_maintenance = 1;
		break;
	case OPT_LEG_TOKENS:
		strict_leg_tokens = 1;
		break;
	}
}

static void config_set_defaults(void)
{
	/* Set defaults for string values. */
	config_set_string_default(&config_pid_path, MEDIATOR_DEFAULT_PIDPATH);
	config_set_string_default(&config_med_host, MEDIATOR_DEFAULT_ACCHOST);
	config_set_string_default(&config_med_user, MEDIATOR_DEFAULT_ACCUSER);
	config_set_string_default(&config_med_pass, MEDIATOR_DEFAULT_ACCPASS);
	config_set_string_default(&config_med_db, MEDIATOR_DEFAULT_ACCDB);
	config_set_string_default(&config_cdr_host, MEDIATOR_DEFAULT_CDRHOST);
	config_set_string_default(&config_cdr_user, MEDIATOR_DEFAULT_CDRUSER);
	config_set_string_default(&config_cdr_pass, MEDIATOR_DEFAULT_CDRPASS);
	config_set_string_default(&config_cdr_db, MEDIATOR_DEFAULT_CDRDB);
	config_set_string_default(&config_prov_host, MEDIATOR_DEFAULT_PROVHOST);
	config_set_string_default(&config_prov_user, MEDIATOR_DEFAULT_PROVUSER);
	config_set_string_default(&config_prov_pass, MEDIATOR_DEFAULT_PROVPASS);
	config_set_string_default(&config_prov_db, MEDIATOR_DEFAULT_PROVDB);
	config_set_string_default(&config_stats_host, MEDIATOR_DEFAULT_STATSHOST);
	config_set_string_default(&config_stats_user, MEDIATOR_DEFAULT_STATSUSER);
	config_set_string_default(&config_stats_pass, MEDIATOR_DEFAULT_STATSPASS);
	config_set_string_default(&config_stats_db, MEDIATOR_DEFAULT_STATSDB);
}

static int config_parse_line(char *line)
{
	char *sep = line;
	const char *value;
	struct option *option;

	for (sep = line; *sep; sep++)
		if (isspace(*sep) || *sep == '=')
			break;
	*sep = '\0';
	for (value = sep; *value; value++)
		if (!isspace(*value) && *value != '=')
			break;

	for (option = long_options; option->name; option++)
		if (strcmp(option->name, line) == 0)
			break;

	if (option->name == NULL)
		return -1;

	if (option->has_arg == no_argument && value != NULL)
		return -1;
	else if (option->has_arg == required_argument && value == NULL)
		return -1;

	config_set_option(option->val, value);

	return 0;
}

static int config_parse_file(const char *filename)
{
	FILE *conffile;
	char *line = NULL;
	size_t len = 0;
	ssize_t nread;

	conffile = fopen(filename, "r");
	if (conffile == NULL) {
		if (errno == ENOENT)
			return 0;
		return -1;
	}

	while ((nread = getline(&line, &len, conffile)) < 0)
		if (config_parse_line(line) < 0)
			return -1;

	free(line);
	fclose(conffile);

	return 0;
}

static int config_parse_cmdopts(int argc, char **argv)
{
	int c;

	while ((c = getopt_long(argc, argv, options, long_options, NULL)) != -1)
	{
		if (c == '?' || c == ':')
			config_help(argv[0], 0);
		else
			config_set_option(c, optarg);
	}

	return 0;
}

int config_parse(const char *filename, int argc, char **argv)
{
	int rc = 0;

	rc |= config_parse_file(filename);
	rc |= config_parse_cmdopts(argc, argv);
	config_set_defaults();

	return rc;
}

void config_cleanup()
{
	free(config_pid_path);
	free(config_cdr_host);
	free(config_cdr_user);
	free(config_cdr_pass);
	free(config_cdr_db);
	free(config_med_host);
	free(config_med_user);
	free(config_med_pass);
	free(config_med_db);
	free(config_prov_host);
	free(config_prov_user);
	free(config_prov_pass);
	free(config_prov_db);
	free(config_stats_host);
	free(config_stats_user);
	free(config_stats_pass);
	free(config_stats_db);
}
