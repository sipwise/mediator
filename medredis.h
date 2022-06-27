#ifndef _MED_REDIS_H
#define _MED_REDIS_H

#include <glib.h>

#include "mediator.h"
#include "cdr.h"

int medredis_init(void);
void medredis_cleanup(void);
med_callid_t *medredis_fetch_callids(uint64_t *count);
int medredis_fetch_records(med_callid_t *callid, med_entry_t **entries, uint64_t *count);
int medredis_trash_entries(med_entry_t **records, uint64_t *count);
int medredis_backup_entries(med_entry_t **records, uint64_t *count);
int medredis_batch_end(void);

#endif /* _MED_REDIS_H */
