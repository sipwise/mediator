#ifndef _MED_MYSQL_H
#define _MED_MYSQL_H

#include <glib.h>

#include "mediator.h"
#include "cdr.h"

#define STAT_PERIOD_SIZE 30

struct medmysql_call_stat_info_t {
	char period[STAT_PERIOD_SIZE];
	char call_code[4];
	u_int64_t amount;
};

int medmysql_init();
void medmysql_cleanup();
med_callid_t *medmysql_fetch_callids(u_int64_t *count);
int medmysql_fetch_records(med_callid_t *callid, med_entry_t **entries, u_int64_t *count);
int medmysql_trash_entries(const char *callid);
int medmysql_backup_entries(const char *callid);
int medmysql_delete_entries(const char *callid);
int medmysql_insert_cdrs(cdr_entry_t *records, u_int64_t count);
int medmysql_load_maps(GHashTable *ip_table, GHashTable *host_table, GHashTable *id_table);
int medmysql_load_uuids(GHashTable *uuid_table);
int medmysql_batch_start();
int medmysql_batch_end();
int medmysql_update_call_stat_info(const char *call_code, const double start_time);

#endif /* _MED_MYSQL_H */
