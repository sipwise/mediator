#ifndef _MED_REDIS_H
#define _MED_REDIS_H

#include <glib.h>

#include "mediator.h"
#include "cdr.h"

int medredis_init(void);
void medredis_cleanup(void);
gboolean medredis_fetch_callids(GQueue *output);
int medredis_fetch_records(char *callid, GQueue *entries, records_filter_func, void *filter_data);
int medredis_trash_entries(GQueue *records);
int medredis_backup_entries(GQueue *records);

#endif /* _MED_REDIS_H */
