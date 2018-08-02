#ifndef _RECORDS_H
#define _RECORDS_H

#include "mediator.h"

void records_sort(med_entry_t *records, uint64_t count);
int records_complete(med_entry_t *records, uint64_t count);

#endif
