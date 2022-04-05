#include "records.h"
#include "config.h"
#include <glib.h>
#include <time.h>

#define comp_ret(a_var, b_var) \
    do { \
        __auto_type x_a = a_var; \
        __auto_type x_b = b_var; \
        if (x_a < x_b) \
            return -1; \
        if (x_a > x_b) \
            return 1; \
    } while (0)

static int records_sort_func(const void *aa, const void *bb, __attribute__ ((unused)) void *dummy)
{
    const med_entry_t *a = aa;
    const med_entry_t *b = bb;

    comp_ret(strlen(a->callid), strlen(b->callid));
    comp_ret(a->unix_timestamp, b->unix_timestamp);
    return 0;
}

void records_sort(GQueue *records)
{
    g_queue_sort(records, records_sort_func, NULL);
}

int records_complete(GQueue *records)
{
    uint8_t has_bye = 0;
    uint8_t has_inv_200 = 0;

    // if our records are old enough, we always consider them complete
    if (records->head && config_max_acc_age)
    {
        med_entry_t *r = g_queue_peek_head(records);
        if (time(NULL) - r->unix_timestamp > config_max_acc_age) {
            r->timed_out = 1;
            return 1;
        }
    }

    for (GList *l = records->head; l; l = l->next)
    {
        med_entry_t *s = l->data;
        if (s->sip_method[0] == 'I' && s->sip_method[1] == 'N' && s->sip_method[2] == 'V' &&
                s->sip_code[0] == '2') {
            has_inv_200 |= 1;
        } else if (s->sip_method[0] == 'B' && s->sip_method[1] == 'Y' && s->sip_method[2] == 'E') {
            has_bye |= 1;
        }
    }
    if (has_inv_200 && !has_bye)
        return 0;
    return 1;
}
