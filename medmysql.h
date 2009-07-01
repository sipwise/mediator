#ifndef _MED_MYSQL_H
#define _MED_MYSQL_H

#include <glib.h>

#include "mediator.h"
#include "cdr.h"

int medmysql_init();
void medmysql_cleanup();
int medmysql_fetch_callids(med_callid_t **callids, u_int64_t *count);
int medmysql_fetch_records(med_callid_t *callid, med_entry_t **entries, u_int64_t *count);
int medmysql_trash_entries(const char *callid);
int medmysql_backup_entries(const char *callid);
int medmysql_delete_entries(const char *callid);
int medmysql_insert_cdrs(cdr_entry_t *records, u_int64_t count);
int medmysql_load_maps(GHashTable *host_table, GHashTable *ip_table);
int medmysql_load_uuids(GHashTable *uuid_table);

#endif /* _MED_MYSQL_H */
