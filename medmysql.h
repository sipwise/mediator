#ifndef _MED_MYSQL_H
#define _MED_MYSQL_H

#include <glib.h>

#include "mediator.h"
#include "cdr.h"

#define PACKET_SIZE (1024*1024)

struct medmysql_str {
	char str[PACKET_SIZE];
	unsigned int len;
};

struct medmysql_batches {
	struct medmysql_str cdrs;
	struct medmysql_str acc_backup;
	struct medmysql_str acc_trash;
	struct medmysql_str to_delete;
};

int medmysql_init();
void medmysql_cleanup();
int medmysql_fetch_callids(med_callid_t **callids, u_int64_t *count);
int medmysql_fetch_records(med_callid_t *callid, med_entry_t **entries, u_int64_t *count);
int medmysql_trash_entries(const char *callid, struct medmysql_batches *);
int medmysql_backup_entries(const char *callid, struct medmysql_batches *);
int medmysql_delete_entries(const char *callid, struct medmysql_batches *);
int medmysql_insert_cdrs(cdr_entry_t *records, u_int64_t count, struct medmysql_batches *);
int medmysql_load_maps(GHashTable *ip_table, GHashTable *host_table, GHashTable *id_table);
int medmysql_load_uuids(GHashTable *uuid_table);
int medmysql_batch_start(struct medmysql_batches *);
int medmysql_batch_end(struct medmysql_batches *);
int medmysql_update_call_stat_info(const char *call_code, const double start_time);

#endif /* _MED_MYSQL_H */
