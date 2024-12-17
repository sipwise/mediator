#ifndef _CONFIG_H
#define _CONFIG_H

#include <stdint.h>
#include "mediator.h"

extern uint8_t config_daemonize;
extern char *config_pid_path;
extern char *config_hostname;

extern char *config_med_host;
extern unsigned int config_med_port;
extern char *config_med_user;
extern char *config_med_pass;
extern char *config_med_db;

extern char *config_redis_host;
extern unsigned int config_redis_port;
extern char *config_redis_pass;
extern unsigned int config_redis_db;

extern char *config_cdr_host;
extern unsigned int config_cdr_port;
extern char *config_cdr_user;
extern char *config_cdr_pass;
extern char *config_cdr_db;
extern char *config_intermediate_cdr_host;
extern unsigned int config_intermediate_cdr_port;
extern char *config_cdr_error_file;

extern char *config_prov_host;
extern unsigned int config_prov_port;
extern char *config_prov_user;
extern char *config_prov_pass;
extern char *config_prov_db;

extern char *config_stats_host;
extern unsigned int config_stats_port;
extern char *config_stats_user;
extern char *config_stats_pass;
extern char *config_stats_db;
extern med_stats_period_t config_stats_period;

extern unsigned int config_interval;
extern uint8_t config_dumpcdr;

extern int config_maintenance;
extern int strict_leg_tokens;
extern int config_max_acc_age;
extern int config_intermediate_interval;

extern med_loglevel_t config_loglevel;

int config_parse(int argc, char **argv);
void config_cleanup(void);

#endif /* _CONFIG_H */
