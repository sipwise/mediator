#ifndef _MEDIATOR_H
#define _MEDIATOR_H 

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <errno.h>
#include <syslog.h>
#include <unistd.h>
#include <stdlib.h>
#include <glib.h>
#include <inttypes.h>
#include <json.h>

/* Compiler support. */

/* Supported since gcc 5.1.0 and clang 2.9.0. */
#ifndef __has_attribute
#define __has_attribute(x)	0
#endif

#if __has_attribute(__format__)
#define MEDIATOR_ATTR_FORMAT(type, fmt, args) __attribute__((__format__(type, fmt, args)))
#define MEDIATOR_ATTR_PRINTF(fmt, args) MEDIATOR_ATTR_FORMAT(__printf__, fmt, args)
#else
#define MEDIATOR_ATTR_FORMAT(type, fmt, args)
#define MEDIATOR_ATTR_PRINTF(fmt, args)
#endif

#define MEDIATOR_DEFAULT_INTERVAL 10

#define MEDIATOR_SYSLOG_NAME "mediator"
#define MEDIATOR_LOCK_FILE "/run/lock/mediator.lock"
#define MEDIATOR_DEFAULT_CONFIG_FILE "/etc/ngcp-mediator/ngcp-mediator.conf"
#define MEDIATOR_DEFAULT_DAEMONIZE 0
#define MEDIATOR_DEFAULT_DUMPCDR 0
#define MEDIATOR_DEFAULT_PIDPATH "/run/mediator.pid"

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

#define MEDIATOR_DEFAULT_STATSHOST "localhost"
#define MEDIATOR_DEFAULT_STATSUSER "mediator"
#define MEDIATOR_DEFAULT_STATSPASS "GimmeAllUr$$$"
#define MEDIATOR_DEFAULT_STATSDB   "stats"
#define MEDIATOR_DEFAULT_STATSPORT 0
#define MEDIATOR_DEFAULT_STATSPERIOD MED_STATS_HOUR

#define MEDIATOR_DEFAULT_REDISHOST "localhost"
#define MEDIATOR_DEFAULT_REDISPORT 6379
#define MEDIATOR_DEFAULT_REDISDB   21

#define MEDIATOR_DEFAULT_LOGLEVEL MED_LOG_INFO

#define MED_GW_STRING "gw"
#define MED_AS_STRING "as"
#define MED_PEER_STRING "peer"

#define MED_MIN_BASELEN 6

#define MED_SEP '|'

#define PBXSUFFIX "_pbx-1"
#define XFERSUFFIX "_xfer-1"

extern int mediator_lockfd;
extern sig_atomic_t mediator_shutdown;

typedef enum {
    MED_UNRECOGNIZED = 0,
    MED_INVITE,
    MED_BYE,
    MED_REFER,
} med_method_t;


typedef struct {
    char *src_leg;
    char *dst_leg;
    json_object *src_leg_json;
    json_object *dst_leg_json;
    char sip_code[4];
    char sip_reason[32];
    char *callid;
    char timestamp[24];
    double unix_timestamp;
    char branch_id[3];
    uint8_t valid;
    med_method_t method;
    char sip_method[32];
    uint8_t redis;
    char *acc_ref;
    uint8_t timed_out;
} med_entry_t;

typedef struct {
    char *str_value;
    time_t created;
} med_cache_entry_t;

typedef gboolean (*records_filter_func)(med_entry_t *, void *data);

extern GHashTable *med_peer_host_table;
extern GHashTable *med_peer_ip_table;
extern GHashTable *med_peer_id_table;
extern GHashTable *med_uuid_cache;
extern GHashTable *med_call_stat_info_table;
extern GHashTable *med_cdr_tag_table;


void critical(const char *);

void med_entry_free(void *p);

static inline int check_shutdown(void) {
    if (mediator_shutdown) {
        syslog(LOG_INFO, "Shutdown detected, aborting work in progress");
        return 1;
    }
    return 0;
}

typedef enum {
    MED_STATS_HOUR  = 1,
    MED_STATS_DAY   = 2,
    MED_STATS_MONTH = 3
} med_stats_period_t;

typedef enum {
    MED_LOG_EMERGENCY = 0,
    MED_LOG_ALERT     = 1,
    MED_LOG_CRITICAL  = 2,
    MED_LOG_ERROR     = 3,
    MED_LOG_WARNING   = 4,
    MED_LOG_NOTICE    = 5,
    MED_LOG_INFO      = 6,
    MED_LOG_DEBUG     = 7
} med_loglevel_t;

#define _LOG(level, fmt, args...) \
    do { \
        syslog((level), "%s:%d [%s]: " fmt, __FILE__, __LINE__, __func__, ##args); \
    } while(0)

#define L_DEBUG(fmt, args...)     do { if (config_loglevel >= MED_LOG_DEBUG) { _LOG(LOG_DEBUG,   fmt, ##args); } } while(0)
#define L_INFO(fmt, args...)      do { if (config_loglevel >= MED_LOG_INFO) { _LOG(LOG_INFO,   fmt, ##args); } } while(0)
#define L_NOTICE(fmt, args...)    do { if (config_loglevel >= MED_LOG_NOTICE) { _LOG(LOG_NOTICE,   fmt, ##args); } } while(0)
#define L_WARNING(fmt, args...)   do { if (config_loglevel >= MED_LOG_WARNING) { _LOG(LOG_WARNING,   fmt, ##args); } } while(0)
#define L_ERROR(fmt, args...)     do { if (config_loglevel >= MED_LOG_ERROR) { _LOG(LOG_ERR,   fmt, ##args); } } while(0)
#define L_CRITICAL(fmt, args...)  do { if (config_loglevel >= MED_LOG_CRITICAL) { _LOG(LOG_CRIT,   fmt, ##args); } } while(0)
#define L_ALERT(fmt, args...)     do { if (config_loglevel >= MED_LOG_ALERT) { _LOG(LOG_ALERT,   fmt, ##args); } } while(0)
#define L_EMERGENCY(fmt, args...) do { if (config_loglevel >= MED_LOG_EMERG) { _LOG(LOG_EMERG,   fmt, ##args); } } while(0)


#endif /* _MEDIATOR_H */
