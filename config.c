#include <getopt.h>

#include "config.h"
#include "mediator.h"

unsigned int config_interval = MEDIATOR_DEFAULT_INTERVAL;
u_int8_t config_dumpcdr = MEDIATOR_DEFAULT_DUMPCDR;
u_int8_t config_daemonize = MEDIATOR_DEFAULT_DAEMONIZE;
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

static void config_help(const char *self, int rc)
{
	printf("mediator 1.2.0 - Creates call detail records from OpenSER accounting.\n" \
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
		self, MEDIATOR_DEFAULT_PIDPATH, MEDIATOR_DEFAULT_INTERVAL,
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

static void config_set_string_option(char **str, char *opt)
{
	free(*str);
	*str = strdup(opt);
}

static void config_set_string_default(char **str, char *def)
{
	if (*str == NULL)
		*str = strdup(def);
}

int config_parse_cmdopts(int argc, char **argv)
{
	int c;

	while((c = getopt(argc, argv, "D:i:dl?h:u:p:b:o:H:U:P:B:O:S:T:R:A:N:Z:z:W:w:X:x:ms")) != -1)
	{
		if(c == '?' || c == ':')
			config_help(argv[0], 0);
		else if(c == 'd')
			config_daemonize = 1;
		else if(c == 'l')
			config_dumpcdr = 1;
		else if(c == 'D')
			config_set_string_option(&config_pid_path, optarg);
		else if(c == 'i')
			config_interval = atoi(optarg);
		else if(c == 'h')
			config_set_string_option(&config_med_host, optarg);
		else if(c == 'u')
			config_set_string_option(&config_med_user, optarg);
		else if(c == 'p')
			config_set_string_option(&config_med_pass, optarg);
		else if(c == 'b')
			config_set_string_option(&config_med_db, optarg);
		else if(c == 'o')
			config_med_port = atoi(optarg);
		else if(c == 'H')
			config_set_string_option(&config_cdr_host, optarg);
		else if(c == 'U')
			config_set_string_option(&config_cdr_user, optarg);
		else if(c == 'P')
			config_set_string_option(&config_cdr_pass, optarg);
		else if(c == 'B')
			config_set_string_option(&config_cdr_db, optarg);
		else if(c == 'O')
			config_cdr_port = atoi(optarg);
		else if(c == 'S')
			config_set_string_option(&config_prov_host, optarg);
		else if(c == 'R')
			config_set_string_option(&config_prov_user, optarg);
		else if(c == 'A')
			config_set_string_option(&config_prov_pass, optarg);
		else if(c == 'N')
			config_set_string_option(&config_prov_db, optarg);
		else if(c == 'T')
			config_prov_port = atoi(optarg);
		else if(c == 'Z')
			config_set_string_option(&config_stats_host, optarg);
		else if(c == 'z')
			config_stats_port = atoi(optarg);
		else if(c == 'W')
			config_set_string_option(&config_stats_user, optarg);
		else if(c == 'w')
			config_set_string_option(&config_stats_pass, optarg);
		else if(c == 'X')
			config_set_string_option(&config_stats_db, optarg);
		else if(c == 'x')
			config_stats_period = (med_stats_period_t)atoi(optarg);
		else if(c == 'm')
			config_maintenance = 1;
		else if(c == 's')
			strict_leg_tokens = 1;
	}

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

	return 0;
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
