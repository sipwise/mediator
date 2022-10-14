#ifndef _MED_MYSQL_H
#define _MED_MYSQL_H

#include <glib.h>

#include "mediator.h"
#include "cdr.h"

#define PACKET_SIZE (1024*1024)

#define STAT_PERIOD_SIZE 30

struct _medmysql_batch_definition;
struct medmysql_batches;
struct medmysql_cdr_batch;

struct medmysql_str {
    char str[PACKET_SIZE];
    size_t len;
    const struct _medmysql_batch_definition *def;
    struct medmysql_batches *batches;
    struct medmysql_cdr_batch *cdr_batch;
    GQueue q;
};

struct medmysql_cdr_batch {
    struct medmysql_str cdrs;
    struct medmysql_str tags;
    struct medmysql_str mos;
    struct medmysql_str group;
    unsigned long num_cdrs;
};

struct medmysql_batches {
    struct medmysql_cdr_batch cdr_batch;
    struct medmysql_cdr_batch int_cdr_batch;

    struct medmysql_str acc_backup;
    struct medmysql_str acc_trash;
    struct medmysql_str to_delete;
    struct medmysql_str int_cdr_delete;
};

struct medmysql_call_stat_info_t {
    char period[STAT_PERIOD_SIZE];
    char call_code[4];
    uint64_t amount;
};

int medmysql_init(void);
void medmysql_cleanup(void);
gboolean medmysql_fetch_callids(GQueue *output);
int medmysql_fetch_records(char *callid, GQueue *entries, int warn_empty, records_filter_func, void *filter_data);
int medmysql_trash_entries(const char *callid, struct medmysql_batches *);
int medmysql_backup_entries(const char *callid, struct medmysql_batches *);
int medmysql_delete_entries(const char *callid, struct medmysql_batches *);
int medmysql_insert_cdrs(cdr_entry_t *records, uint64_t count, struct medmysql_batches *);
int medmysql_delete_intermediate(cdr_entry_t *records, uint64_t count, struct medmysql_batches *);
int medmysql_load_maps(GHashTable *ip_table, GHashTable *host_table, GHashTable *id_table);
char *medmysql_lookup_uuid(const char *uuid);
int medmysql_load_db_ids(void);
int medmysql_load_cdr_tag_ids(GHashTable *cdr_tag_table);
int medmysql_batch_start(struct medmysql_batches *);
int medmysql_batch_end(struct medmysql_batches *);
int medmysql_update_call_stat_info(const char *call_code, const double start_time);
int medmysql_insert_records(GQueue *records, const char *table);
void medmysql_cache_cleanup(GHashTable *);

#endif /* _MED_MYSQL_H */
