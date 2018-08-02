#include "records.h"
#include "config.h"

#define comp_ret(a_var, b_var) \
    do { \
        __auto_type x_a = a_var; \
        __auto_type x_b = b_var; \
        if (x_a < x_b) \
            return -1; \
        if (x_a > x_b) \
            return 1; \
    } while (0)

static int records_sort_func(const void *aa, const void *bb)
{
    const med_entry_t *a = aa;
    const med_entry_t *b = bb;

    comp_ret(strlen(a->callid), strlen(b->callid));
    comp_ret(a->unix_timestamp, b->unix_timestamp);
    return 0;
}

void records_sort(med_entry_t *records, uint64_t count)
{
    qsort(records, count, sizeof(med_entry_t), records_sort_func);
}

int records_complete(med_entry_t *records, uint64_t count)
{
    uint8_t has_bye = 0;
    uint8_t has_inv_200 = 0;

    for (uint64_t i = 0; i < count; i++)
    {
        med_entry_t *s = &records[i];
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
