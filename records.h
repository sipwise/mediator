#ifndef _RECORDS_H
#define _RECORDS_H

#include "mediator.h"
#include <glib.h>

void records_sort(GQueue *records);
int records_complete(GQueue *records);
int records_handle_refer(GQueue *records, med_entry_t *, const char *callid);
gboolean records_no_refer_bleg_bye(med_entry_t *e, void *data);

#endif
