#ifndef _MEDIATOR_H
#define _MEDIATOR_H 

#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <errno.h>
#include <syslog.h>
#include <unistd.h>
#include <stdlib.h>
#include <glib.h>
#include <inttypes.h>


#define MEDIATOR_DEFAULT_INTERVAL 10

#define MEDIATOR_SYSLOG_NAME "mediator"
#define MEDIATOR_LOCK_FILE "/var/lock/mediator.lock"
#define MEDIATOR_DEFAULT_DAEMONIZE 0
#define MEDIATOR_DEFAULT_DUMPCDR 0
#define MEDIATOR_DEFAULT_PIDPATH "/var/run/mediator.pid"

#define MEDIATOR_DEFAULT_ACCHOST "localhost"
#define MEDIATOR_DEFAULT_ACCUSER "mediator"
#define MEDIATOR_DEFAULT_ACCPASS "GimmeAllUr$$$"
#define MEDIATOR_DEFAULT_ACCDB   "accounting"
#define MEDIATOR_DEFAULT_ACCPORT 0

#define MEDIATOR_DEFAULT_CDRHOST "localhost"
#define MEDIATOR_DEFAULT_CDRUSER "mediator"
#define MEDIATOR_DEFAULT_CDRPASS "GimmeAllUr$$$"
#define MEDIATOR_DEFAULT_CDRDB   "accounting"
#define MEDIATOR_DEFAULT_CDRPORT 0

#define MEDIATOR_DEFAULT_PROVHOST "localhost"
#define MEDIATOR_DEFAULT_PROVUSER "mediator"
#define MEDIATOR_DEFAULT_PROVPASS "GimmeAllUr$$$"
#define MEDIATOR_DEFAULT_PROVDB   "provisioning"
#define MEDIATOR_DEFAULT_PROVPORT 0

#define MED_GW_STRING "gw"
#define MED_AS_STRING "as"
#define MED_PEER_STRING "peer"

#define MED_MIN_BASELEN 6

#define MED_SEP '|'

extern int mediator_lockfd;

typedef enum {
	MED_INVITE = 1,
	MED_BYE = 2,
	MED_UNRECOGNIZED = 3
} med_method_t;


typedef struct {
	char src_leg[256];
	char dst_leg[256];
	char sip_code[4];
	char sip_reason[32];
	char callid[128];
	char timestamp[24];
	u_int64_t unix_timestamp;
	u_int64_t med_id;
	u_int8_t valid;
	med_method_t method;
	char sip_method[32];
} med_entry_t;

typedef struct {
	char value[128];
} med_callid_t;

extern GHashTable *med_peer_host_table;
extern GHashTable *med_peer_ip_table;
extern GHashTable *med_uuid_table;


void critical(const char *);


#endif /* _MEDIATOR_H */
