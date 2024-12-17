#include <ctype.h>
#include <getopt.h>

#include "config.h"
#include "mediator.h"

char *config_file_path;
unsigned int config_interval = MEDIATOR_DEFAULT_INTERVAL;
uint8_t config_dumpcdr = MEDIATOR_DEFAULT_DUMPCDR;
uint8_t config_daemonize = MEDIATOR_DEFAULT_DAEMONIZE;
char *config_pid_path;

char *config_med_host;
char *config_med_user;
char *config_med_pass;
char *config_med_db;
unsigned int config_med_port = MEDIATOR_DEFAULT_ACCPORT;

char *config_redis_host;
unsigned int config_redis_port = MEDIATOR_DEFAULT_REDISPORT;
unsigned int config_redis_db = MEDIATOR_DEFAULT_REDISDB;
char *config_redis_pass = NULL;

char *config_cdr_host;
char *config_cdr_user;
char *config_cdr_pass;
char *config_cdr_db;
unsigned int config_cdr_port = MEDIATOR_DEFAULT_CDRPORT;
char *config_intermediate_cdr_host;
unsigned int config_intermediate_cdr_port = MEDIATOR_DEFAULT_CDRPORT;
char *config_cdr_error_file;

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
int config_max_acc_age = 0;
int config_intermediate_interval = 0;

med_loglevel_t config_loglevel = MEDIATOR_DEFAULT_LOGLEVEL;

enum config_option {
    OPT_CONFIGFILE = 'c',
    OPT_DAEMONIZE = 'd',
    OPT_PIDFILE = 'D',
    OPT_SYSLOG = 'l',
    OPT_LOGLEVEL = 'L',
    OPT_INTERVAL = 'i',
    OPT_MED_HOST = 'h',
    OPT_MED_PORT = 'o',
    OPT_MED_USER = 'u',
    OPT_MED_PASS = 'p',
    OPT_MED_DB = 'b',
    OPT_CDR_HOST = 'H',
    OPT_CDR_PORT = 'O',
    OPT_CDR_USER = 'U',
    OPT_CDR_PASS = 'P',
    OPT_CDR_DB = 'B',
    OPT_INTERMEDIATE_CDR_HOST = 'y',
    OPT_INTERMEDIATE_CDR_PORT = 'Y',
    OPT_CDR_ERROR_FILE = 'E',
    OPT_PROV_HOST = 'S',
    OPT_PROV_PORT = 'T',
    OPT_PROV_USER = 'R',
    OPT_PROV_PASS = 'A',
    OPT_PROV_DB = 'N',
    OPT_STATS_HOST = 'Z',
    OPT_STATS_PORT = 'z',
    OPT_STATS_USER = 'W',
    OPT_STATS_PASS = 'w',
    OPT_STATS_DB = 'X',
    OPT_REDIS_HOST = 'a',
    OPT_REDIS_PORT = 't',
    OPT_REDIS_DB = 'r',
    OPT_REDIS_PASS = 'e',
    OPT_STATS_PERIOD = 'x',
    OPT_MAINTENANCE = 'm',
    OPT_LEG_TOKENS = 's',
    OPT_MAX_ACC_AGE = 'M',
    OPT_INTERMEDIATE_INTERVAL = 'I',
};

static const char options[] = "?a:c:e:D:i:dlL:h:u:p:b:o:H:U:P:B:O:S:t:T:r:R:A:N:Z:z:W:w:X:x:msM:I:y:Y:";

struct option long_options[] = {
    { "configfile", required_argument, NULL, OPT_CONFIGFILE },
    { "pidfile", required_argument, NULL, OPT_PIDFILE },
    { "daemonize", no_argument, NULL, OPT_DAEMONIZE },
    { "logquery", no_argument, NULL, OPT_SYSLOG },
    { "loglevel",  required_argument, NULL, OPT_LOGLEVEL },
    { "interval", required_argument, NULL, OPT_INTERVAL },
    { "med-host", required_argument, NULL, OPT_MED_HOST },
    { "med-port", required_argument, NULL, OPT_MED_PORT },
    { "med-user", required_argument, NULL, OPT_MED_USER },
    { "med-pass", required_argument, NULL, OPT_MED_PASS },
    { "med-db", required_argument, NULL, OPT_MED_DB },
    { "cdr-host", required_argument, NULL, OPT_CDR_HOST },
    { "cdr-port", required_argument, NULL, OPT_CDR_PORT },
    { "cdr-user", required_argument, NULL, OPT_CDR_USER },
    { "cdr-pass", required_argument, NULL, OPT_CDR_PASS },
    { "cdr-db", required_argument, NULL, OPT_CDR_DB },
    { "intermediate-cdr-host", required_argument, NULL, OPT_INTERMEDIATE_CDR_HOST },
    { "intermediate-cdr-port", required_argument, NULL, OPT_INTERMEDIATE_CDR_PORT },
    { "cdr-error-file", required_argument, NULL, OPT_CDR_ERROR_FILE },
    { "prov-host", required_argument, NULL, OPT_PROV_HOST },
    { "prov-port", required_argument, NULL, OPT_PROV_PORT },
    { "prov-user", required_argument, NULL, OPT_PROV_USER },
    { "prov-pass", required_argument, NULL, OPT_PROV_PASS },
    { "prov-db", required_argument, NULL, OPT_PROV_DB },
    { "stats-host", required_argument, NULL, OPT_STATS_HOST },
    { "stats-port", required_argument, NULL, OPT_STATS_PORT },
    { "stats-user", required_argument, NULL, OPT_STATS_USER },
    { "stats-pass", required_argument, NULL, OPT_STATS_PASS },
    { "stats-db", required_argument, NULL, OPT_STATS_DB },
    { "redis-host", required_argument, NULL, OPT_REDIS_HOST },
    { "redis-port", required_argument, NULL, OPT_REDIS_PORT },
    { "redis-db", required_argument, NULL, OPT_REDIS_DB },
    { "redis-pass", required_argument, NULL, OPT_REDIS_PASS },
    { "stats-period", required_argument, NULL, OPT_STATS_PERIOD },
    { "maintenance", no_argument, NULL, OPT_MAINTENANCE },
    { "leg-tokens", no_argument, NULL, OPT_LEG_TOKENS },
    { "max-acc-age", required_argument, NULL, OPT_MAX_ACC_AGE },
    { "intermediate-interval", required_argument, NULL, OPT_INTERMEDIATE_INTERVAL },
    { NULL, 0, NULL, 0 },
};

static void config_help(const char *self, int rc)
{
    printf(
"mediator %s - Creates call detail records from OpenSER accounting.\n" \
"Usage: %s [<option>...]\n" \
"\n" \
"Options:\n" \
"  -c, --configfile FILE\tThe config file to use (default = '%s').\n" \
"  -D, --pidfile PIDFILE\tThe path of the PID file (default = '%s').\n" \
"  -d, --daemonize\tEnables daemonization of the process (default = disabled).\n" \
"  -l, --logquery\t\tEnables logging of CDR sql query to file (default = disabled).\n" \
"  -L, --loglevel\tThe loglevel from 7=debug to 0=emergency (default = %d).\n" \
"  -i, --interval INT\tThe creation interval (default = %d).\n" \
"  -h, --med-host HOST\tThe ACC db host (default = '%s').\n" \
"  -o, --med-port PORT\tThe ACC db port (default = '%d').\n" \
"  -u, --med-user USER\tThe ACC db user (default = '%s').\n" \
"  -p, --med-pass PASS\tThe ACC db pass (default = '%s').\n" \
"  -b, --med-db DB\tThe ACC db name (default = '%s').\n" \
"  -H, --cdr-host HOST\tThe CDR db host (default = '%s').\n" \
"  -O, --cdr-port PORT\tThe CDR db port (default = '%d').\n" \
"  -U, --cdr-user USER\tThe CDR db user (default = '%s').\n" \
"  -P, --cdr-pass PASS\tThe CDR db pass (default = '%s').\n" \
"  -B, --cdr-db DB\tThe CDR db name (default = '%s').\n" \
"  -y, --intermediate-cdr-host HOST\tThe CDR db host (default = '%s').\n" \
"  -Y, --intermediate-cdr-port PORT\tThe CDR db port (default = '%d').\n" \
"  -E, --cdr-error-file FILE\tThe CDR db port (default = none).\n" \
"  -S, --prov-host HOST\tThe prov db host (default = '%s').\n" \
"  -T, --prov-port PORT\tThe prov db port (default = '%d').\n" \
"  -R, --prov-user USER\tThe prov db user (default = '%s').\n" \
"  -A, --prov-pass PASS\tThe prov db pass (default = '%s').\n" \
"  -N, --prov-db DB\tThe prov db name (default = '%s').\n" \
"  -Z, --stats-host HOST\tThe stats db host (default = '%s').\n" \
"  -z, --stats-port PORT\tThe stats db port (default = '%d').\n" \
"  -W, --stats-user USER\tThe stats db user (default = '%s').\n" \
"  -w, --stats-pass PASS\tThe stats db pass (default = '%s').\n" \
"  -X, --stats-db DB\tThe stats db name (default = '%s').\n" \
"  -x, --stats-period INT\tThe stats db period (default = '%d', 1=hour, 2=day, 3=month).\n" \
"  -a, --redis-host HOST\tThe redis db host (default = '%s').\n" \
"  -t, --redis-port PORT\tThe redis db port (default = '%d').\n" \
"  -e, --redis-pass PASS\tThe redis db pass (default = none).\n" \
"  -r, --redis-db DB\tThe redis usrloc db number (default = '%d').\n" \
"  -m, --maintenance\tMaintenance mode (do nothing, just sleep).\n" \
"  -s, --leg-tokens\tStrict acc fields (move to trash otherwise).\n" \
"  -M, --max-acc-age\tMaximum age of acc records before trashing them (default = disabled).\n" \
"  -I, --intermediate-interval\tHow often to write/update intermediate CDRs (default = disabled).\n" \
"  -?, --help\t\tDisplays this message.\n",
        MEDIATOR_VERSION, self, MEDIATOR_DEFAULT_CONFIG_FILE,
        MEDIATOR_DEFAULT_PIDPATH, MEDIATOR_DEFAULT_LOGLEVEL,
        MEDIATOR_DEFAULT_INTERVAL,
        MEDIATOR_DEFAULT_ACCHOST, MEDIATOR_DEFAULT_ACCPORT,
        MEDIATOR_DEFAULT_ACCUSER, MEDIATOR_DEFAULT_ACCPASS,
        MEDIATOR_DEFAULT_ACCDB,
        MEDIATOR_DEFAULT_CDRHOST, MEDIATOR_DEFAULT_CDRPORT,
        MEDIATOR_DEFAULT_CDRUSER, MEDIATOR_DEFAULT_CDRPASS,
        MEDIATOR_DEFAULT_CDRDB,
        MEDIATOR_DEFAULT_CDRHOST, MEDIATOR_DEFAULT_CDRPORT,
        MEDIATOR_DEFAULT_PROVHOST, MEDIATOR_DEFAULT_PROVPORT,
        MEDIATOR_DEFAULT_PROVUSER, MEDIATOR_DEFAULT_PROVPASS,
        MEDIATOR_DEFAULT_PROVDB,
        MEDIATOR_DEFAULT_STATSHOST, MEDIATOR_DEFAULT_STATSPORT,
        MEDIATOR_DEFAULT_STATSUSER, MEDIATOR_DEFAULT_STATSPASS,
        MEDIATOR_DEFAULT_STATSDB, MEDIATOR_DEFAULT_STATSPERIOD,
        MEDIATOR_DEFAULT_REDISHOST, MEDIATOR_DEFAULT_REDISPORT,
        MEDIATOR_DEFAULT_REDISDB);

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
    case OPT_LOGLEVEL:
        config_loglevel = atoi(value);
        break;
    case OPT_CONFIGFILE:
        config_set_string_option(&config_file_path, value);
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
    case OPT_INTERMEDIATE_CDR_HOST:
        config_set_string_option(&config_intermediate_cdr_host, value);
        break;
    case OPT_INTERMEDIATE_CDR_PORT:
        config_intermediate_cdr_port = atoi(value);
        break;
    case OPT_CDR_ERROR_FILE:
        config_set_string_option(&config_cdr_error_file, value);
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
    case OPT_REDIS_HOST:
        config_set_string_option(&config_redis_host, value);
        break;
    case OPT_REDIS_PORT:
        config_redis_port = atoi(value);
        break;
    case OPT_REDIS_DB:
        config_redis_db = atoi(value);
        break;
    case OPT_REDIS_PASS:
        config_set_string_option(&config_redis_pass, value);
        break;
    case OPT_MAINTENANCE:
        config_maintenance = 1;
        break;
    case OPT_LEG_TOKENS:
        strict_leg_tokens = 1;
        break;
    case OPT_MAX_ACC_AGE:
        config_max_acc_age = atoi(value);
        break;
    case OPT_INTERMEDIATE_INTERVAL:
        config_intermediate_interval = atoi(value);
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
    config_set_string_default(&config_intermediate_cdr_host, MEDIATOR_DEFAULT_CDRHOST);
    config_set_string_default(&config_prov_host, MEDIATOR_DEFAULT_PROVHOST);
    config_set_string_default(&config_prov_user, MEDIATOR_DEFAULT_PROVUSER);
    config_set_string_default(&config_prov_pass, MEDIATOR_DEFAULT_PROVPASS);
    config_set_string_default(&config_prov_db, MEDIATOR_DEFAULT_PROVDB);
    config_set_string_default(&config_stats_host, MEDIATOR_DEFAULT_STATSHOST);
    config_set_string_default(&config_stats_user, MEDIATOR_DEFAULT_STATSUSER);
    config_set_string_default(&config_stats_pass, MEDIATOR_DEFAULT_STATSPASS);
    config_set_string_default(&config_stats_db, MEDIATOR_DEFAULT_STATSDB);
    config_set_string_default(&config_redis_host, MEDIATOR_DEFAULT_REDISHOST);
}

static int config_parse_line(char *line)
{
    char *sep;
    const char *value;
    struct option *option;

    for (sep = line; *sep; sep++)
        if (isspace(*sep) || *sep == '=')
            break;
    for (value = sep; *value; value++)
        if (!isspace(*value) && *value != '=')
            break;
    if (value != sep)
        *sep = '\0';

    for (option = long_options; option->name; option++)
        if (strcmp(option->name, line) == 0)
            break;

    if (option->name == NULL) {
        L_ERROR("No option found in config file line '%s'", line);
        return -1;
    }

    if (option->has_arg == no_argument && *value != '\0') {
        L_ERROR("Unexpected value in config file option '%s'",
               option->name);
        return -1;
    } else if (option->has_arg == required_argument && *value == '\0') {
        L_ERROR("Missing value in config file option '%s'",
               option->name);
        return -1;
    }

    config_set_option(option->val, value);

    return 0;
}

static int config_parse_file(const char *filename)
{
    FILE *conffile;
    char *line = NULL;
    size_t len = 0;
    ssize_t nread;

    L_DEBUG("Loading config file '%s'", filename);

    conffile = fopen(filename, "r");
    if (conffile == NULL) {
        if (errno == ENOENT)
            return 0;

        L_ERROR("Error loading config file: %s", strerror(errno));
        return -1;
    }

    while ((nread = getline(&line, &len, conffile)) >= 0) {
        if (line[0] == '#')
            continue;

        if (line[nread -1] == '\n')
            line[nread - 1] = '\0';

        if (config_parse_line(line) < 0) {
            L_ERROR("Error parsing config file");
            return -1;
        }
    }

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

int config_parse(int argc, char **argv)
{
    int rc = 0;

    rc |= config_parse_cmdopts(argc, argv);
    config_set_string_default(&config_file_path, MEDIATOR_DEFAULT_CONFIG_FILE);
    rc |= config_parse_file(config_file_path);
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
    free(config_intermediate_cdr_host);
    free(config_cdr_error_file);
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
    free(config_redis_host);
    if (config_redis_pass)
        free(config_redis_pass);
}
