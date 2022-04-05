#ifndef _MED_REDIS_H
#define _MED_REDIS_H

#include <glib.h>

#include "mediator.h"
#include "cdr.h"

int medredis_init(void);
void medredis_cleanup(void);
gboolean medredis_fetch_callids(GQueue *output);
int medredis_fetch_records(char *callid, med_entry_t **entries, uint64_t *count);
int medredis_trash_entries(med_entry_t *records, uint64_t count);
int medredis_backup_entries(med_entry_t *records, uint64_t count);

#endif /* _MED_REDIS_H */
