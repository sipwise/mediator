#ifndef _RECORDS_H
#define _RECORDS_H

#include "mediator.h"
#include <glib.h>

void records_sort(GQueue *records);
int records_complete(GQueue *records);

#endif
