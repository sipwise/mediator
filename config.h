#ifndef _CONFIG_H
#define _CONFIG_H

#include <sys/types.h>
#include "mediator.h"

extern u_int8_t config_daemonize;
extern char *config_pid_path;
extern char *config_hostname;

extern char *config_med_host;
extern unsigned int config_med_port;
extern char *config_med_user;
extern char *config_med_pass;
extern char *config_med_db;

extern char *config_cdr_host;
extern unsigned int config_cdr_port;
extern char *config_cdr_user;
extern char *config_cdr_pass;
extern char *config_cdr_db;

extern char *config_prov_host;
extern unsigned int config_prov_port;
extern char *config_prov_user;
extern char *config_prov_pass;
extern char *config_prov_db;

extern unsigned int config_interval;
extern u_int8_t config_dumpcdr;

int config_parse_cmdopts(int argc, char **argv);
void config_cleanup();

#endif /* _CONFIG_H */
