#include "records.h"
#include "config.h"
#include "medredis.h"
#include "medmysql.h"
#include <glib.h>
#include <time.h>
#include <json.h>

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
        if (s->method == MED_INVITE && s->sip_code[0] == '2') {
            has_inv_200 |= 1;
        } else if (s->method == MED_BYE) {
            has_bye |= 1;
        }
    }
    if (has_inv_200 && !has_bye)
        return 0;
    return 1;
}



static const char *records_get_extras_bleg_cid(med_entry_t *e) {
    if (e->method != MED_BYE)
        return NULL;
    if (!e->dst_leg_json)
        return NULL;
    if (!json_object_is_type(e->dst_leg_json, json_type_object))
        return NULL;

    // dst_leg == "{\"mos\":{\"avg_score\":0.0,\"avg_packetloss\":0,\"avg_jitter\":0,\"avg_rtt\":0},\"extras\":{\"trans_cid_legb\":\"c045e026_pbx-1\"}}"

    json_object *extras;
    if (!json_object_object_get_ex(e->dst_leg_json, "extras", &extras))
        return NULL;
    if (!json_object_is_type(extras, json_type_object))
        return NULL;

    json_object *legb_o;
    if (!json_object_object_get_ex(extras, "trans_cid_legb", &legb_o))
        return NULL;
    if (!json_object_is_type(legb_o, json_type_string))
        return NULL;

    return json_object_get_string(legb_o);
}

static gboolean records_only_refer_bleg_bye(med_entry_t *e, void *data)
{
    const char *callid = data; // original REFER call ID

    const char *legb_cid_o = records_get_extras_bleg_cid(e);
    if (!legb_cid_o)
        return FALSE;

    gboolean keep = FALSE;
    char *legb_cid = g_strdup(legb_cid_o);
    cdr_truncate_call_id_suffix(legb_cid);
    if (strcmp(legb_cid, callid) == 0)
    {
        // call ID points back to self: keep this record
        keep = TRUE;
    }
    g_free(legb_cid);

    return keep;
}

// returns whether any records were fetched, which in turn then requires the list to be sorted
int records_handle_refer(GQueue *records, med_entry_t *e, const char *callid)
{
    if (e->method != MED_REFER)
        return 0;
    if (!e->src_leg_json || !json_object_is_type(e->src_leg_json, json_type_object))
        return 0;
    json_object *so;
    // src_leg == "{\"cid\":\"a6e269eb;to-tag=71974734-624D93330001C401-9D1E0700;from-tag=3d8b44c2b0\"}"
    if (!json_object_object_get_ex(e->src_leg_json, "cid", &so))
        return 0;
    if (!json_object_is_type(so, json_type_string))
        return 0;
    const char *cid_o = json_object_get_string(so);
    if (!cid_o)
        return 0;

    // cid_o is owned by the json object: make a copy
    char *cid = g_strdup(cid_o);

    // truncate URI parameters and call ID suffixes
    char *c = strchr(cid, ';');
    if (c)
        *c = '\0';

    cdr_truncate_call_id_suffix(cid);

    int ret = 0; // was anything added?

    // pull in relevant records for this new call ID: only BYE matching the REFER
    if (medredis_fetch_records(cid, records, records_only_refer_bleg_bye, (void *) callid) > 0)
        ret = 1;
    if (medmysql_fetch_records(cid, records, 0, records_only_refer_bleg_bye, (void *) callid) >= 0)
        ret = 1;

    g_free(cid);

    // was anything added?
    return ret;
}
