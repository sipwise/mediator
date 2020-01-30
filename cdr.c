#include <ctype.h>
#include <json.h>

#include "cdr.h"
#include "medmysql.h"
#include "medredis.h"
#include "config.h"
#include "mediator.h"

static int cdr_create_cdrs(med_entry_t *records, uint64_t count,
        cdr_entry_t **cdrs, uint64_t *cdr_count, uint8_t *trash, int do_intermediate);

static const char* cdr_map_status(const char *sip_status)
{
    if(strncmp("200", sip_status, 3) == 0)
    {
        return CDR_STATUS_OK;
    }
    if(strncmp("480", sip_status, 3) == 0)
    {
        return CDR_STATUS_NA;
    }
    if(strncmp("487", sip_status, 3) == 0)
    {
        return CDR_STATUS_CANCEL;
    }
    if(strncmp("486", sip_status, 3) == 0)
    {
        return CDR_STATUS_BUSY;
    }
    if(strncmp("408", sip_status, 3) == 0)
    {
        return CDR_STATUS_TIMEOUT;
    }
    if(strncmp("404", sip_status, 3) == 0)
    {
        return CDR_STATUS_OFFLINE;
    }

    return CDR_STATUS_UNKNOWN;
}

int cdr_process_records(med_entry_t *records, uint64_t count, uint64_t *ext_count,
        struct medmysql_batches *batches, int do_intermediate)
{
    int ret = 0;
    uint8_t trash = 0;
    uint64_t i;
    int timed_out = 0;

    uint16_t msg_invites = 0;
    uint16_t msg_byes = 0;
    uint16_t msg_unknowns = 0;
    uint16_t invite_200 = 0;

    char *callid = records[0].callid;
    int has_redis = 0, has_mysql = 0;


    cdr_entry_t *cdrs = NULL;
    uint64_t cdr_count;

    *ext_count = 0;

    for(i = 0; i < count; ++i)
    {
        med_entry_t *e = &(records[i]);

        if (e->timed_out)
            timed_out = 1;

        if(!e->valid)
        {
            ++msg_unknowns;
            e->method = MED_UNRECOGNIZED;
        }
        else if(strcmp(e->sip_method, MSG_INVITE) == 0)
        {
            ++msg_invites;
            e->method = MED_INVITE;
            if(strncmp("200", e->sip_code, 3) == 0)
            {
                ++invite_200;
            }
        }
        else if(strcmp(e->sip_method, MSG_BYE) == 0)
        {
            ++msg_byes;
            e->method = MED_BYE;
        }
        else
        {
            L_DEBUG("Unrecognized record with method '%s' for cid '%s'\n", e->sip_method, callid);
            ++msg_unknowns;
            e->method = MED_UNRECOGNIZED;
        }

        if (check_shutdown())
            return 1;

        if (e->redis)
            has_redis = 1;
        else
            has_mysql = 1;
    }

    L_DEBUG("%d INVITEs, %d BYEs, %d unrecognized", msg_invites, msg_byes, msg_unknowns);

    if(msg_invites > 0)
    {
        if(msg_byes > 0 || do_intermediate || timed_out || invite_200 == 0)
        {
            if(/*msg_byes > 2*/ 0)
            {
                L_WARNING("Multiple (%d) BYE messages for callid '%s' found, trashing...",
                        msg_byes, callid);
                trash = 1;
            }
            else
            {
                if(cdr_create_cdrs(records, count, &cdrs, &cdr_count, &trash, do_intermediate) != 0)
                    goto error;
                else
                {
                    *ext_count = cdr_count;
                    if(cdr_count > 0)
                    {
                        if(config_dumpcdr)
                        {
                            /* cdr_log_records(cdrs, cdr_count); */
                        }

                        int insert_ret = medmysql_insert_cdrs(cdrs, cdr_count, batches);

                        if(insert_ret < 0)
                        {
                            goto error;
                        }
                        else if (insert_ret == 0)
                        {
                            if (has_redis)
                            {
                                if(medredis_backup_entries(records, count) != 0)
                                    goto error;
                            }
                            if (has_mysql)
                            {
                                if(medmysql_backup_entries(callid, batches) != 0)
                                    goto error;
                            }
                            if (config_intermediate_interval)
                            {
                                if(medmysql_delete_intermediate(cdrs, cdr_count, batches) != 0)
                                    goto error;
                            }
                        }
                        else if (insert_ret == 1)
                            ; // do nothing - intermediate CDR has been created
                        else
                            goto error;

                    }
                    else
                    {
                        /* TODO: no CDRs created? */
                    }
                    free(cdrs);
                    cdrs = NULL;
                }
            }
        }
        else
        {
            /*
            L_DEBUG("No BYE message for callid '%s' found, skipping...",
                    callid);
            */
            trash = 1;
        }
    }
    else
    {
        /*L_WARNING("No INVITE message for callid '%s' found, trashing...", callid);*/
        trash = 1;
    }

    if(trash)
    {
        if (has_redis)
        {
            if(medredis_trash_entries(records, count) != 0)
                goto error;
        }
        if (has_mysql)
        {
            if(medmysql_trash_entries(callid, batches) != 0)
                goto error;
        }
    }
    return ret;


error:
    if(cdrs)
        free(cdrs);
    return -1;
}

static inline int hexval(unsigned char c) {
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'a' && c <= 'f')
        return 10 + c - 'a';
    if (c >= 'A' && c <= 'F')
        return 10 + c - 'A';
    return -1;
}

/* un-uri-escape string in place */
static void uri_unescape(char *str) {
    unsigned char *s, *d;
    unsigned int c;
    int cv;

    s = (unsigned char *) str;
    d = (unsigned char *) str;

    while (*s) {
        if (*s != '%') {
copy:
            *d = *s;
            d++;
            s++;
            continue;
        }

        cv = hexval(s[1]);
        if (cv == -1)
            goto copy;
        c = cv << 4;
        cv = hexval(s[2]);
        if (cv == -1)
            goto copy;
        c |= cv;
        if (!c) /* disallow null bytes */
            goto copy;

        *d = c;
        s += 3;
        d++;
    }
    *d = 0;
}

static int cdr_parse_json_get_int(json_object *obj, const char *key, int *outp, int min, int max) {
    json_object *int_obj;
    if (!json_object_object_get_ex(obj, key, &int_obj))
        return 0;
    if (!json_object_is_type(int_obj, json_type_int))
        return 0;
    *outp = json_object_get_int64(int_obj);
    if (*outp > max)
        *outp = max;
    else if (*outp < min)
        *outp = min;
    return 1;
}

static int cdr_parse_json_get_double(json_object *obj, const char *key, double *outp, double min, double max) {
    json_object *int_obj;
    if (!json_object_object_get_ex(obj, key, &int_obj))
        return 0;
    if (!json_object_is_type(int_obj, json_type_double))
        return 0;
    *outp = json_object_get_double(int_obj);
    if (*outp > max)
        *outp = max;
    else if (*outp < min)
        *outp = min;
    return 1;
}

static int cdr_parse_json_get_string(json_object *obj, const char *key, char *outp) {
    json_object *str_obj;
    if (!json_object_object_get_ex(obj, key, &str_obj))
        return 0;
    if (!json_object_is_type(str_obj, json_type_string))
        return 0;
    // TODO: if string equals to <null>, null or NULL set it to an empty string '\0'
    g_strlcpy(outp, json_object_get_string(str_obj), sizeof(outp));
    return 1;
}

static int cdr_parse_srcleg(char *srcleg, cdr_entry_t *cdr)
{
    L_DEBUG("Parsing JSON: '%s'", srcleg);

    json_object *json = json_tokener_parse(srcleg);
    if (!json) {
        L_ERROR("Could not parse JSON src_leg string: '%s'", srcleg);
        return -1;
    }
    if (!json_object_is_type(json, json_type_object)) {
        L_ERROR("JSON type is not object: '%s'", srcleg);
        goto err;
    }

    // source_user_id
    if (!cdr_parse_json_get_string(json, "uuid", cdr->source_user_id)) {
        L_ERROR("Call-Id '%s' does not contain 'uuid' key (source user id), '%s'", cdr->call_id, srcleg);
        goto err;
    }

    // source_user
    if (!cdr_parse_json_get_string(json, "u", cdr->source_user)) {
        L_ERROR("Call-Id '%s' does not contain 'u' key (source user), '%s'", cdr->call_id, srcleg);
        goto err;
    }

    // source_domain
    if (!cdr_parse_json_get_string(json, "d", cdr->source_domain)) {
        L_ERROR("Call-Id '%s' does not contain 'd' key (source domain), '%s'", cdr->call_id, srcleg);
        goto err;
    }

    // source_cli
    if (!cdr_parse_json_get_string(json, "cli", cdr->source_cli)) {
        L_ERROR("Call-Id '%s' does not contain 'cli' key (source cli), '%s'", cdr->call_id, srcleg);
        goto err;
    }

    // source_ext_subscriber_id
    if (!cdr_parse_json_get_string(json, "s_id", cdr->source_ext_subscriber_id)) {
        L_ERROR("Call-Id '%s' does not contain 's_id' key (source external subscriber id), '%s'", cdr->call_id, srcleg);
        goto err;
    }
    uri_unescape(cdr->source_ext_subscriber_id);

    // source_ext_contract_id
    if (!cdr_parse_json_get_string(json, "c_id", cdr->source_ext_contract_id)) {
        L_ERROR("Call-Id '%s' does not contain 'c_id' key (source external contract id), '%s'", cdr->call_id, srcleg);
        goto err;
    }
    uri_unescape(cdr->source_ext_contract_id);

    // source_account_id
    if (!cdr_parse_json_get_int(json, "a_id", &cdr->source_account_id, 0, INT_MAX)) {
        L_ERROR("Call-Id '%s' does not contain 'a_id' key (source account id), '%s'", cdr->call_id, srcleg);
        goto err;
    }

    // peer_auth_user
    if (!cdr_parse_json_get_string(json, "pau", cdr->peer_auth_user)) {
        L_ERROR("Call-Id '%s' does not contain 'pau' key (peer auth user), '%s'", cdr->call_id, srcleg);
        goto err;
    }

    // peer_auth_realm
    if (!cdr_parse_json_get_string(json, "par", cdr->peer_auth_realm)) {
        L_ERROR("Call-Id '%s' does not contain 'par' key (peer auth realm), '%s'", cdr->call_id, srcleg);
        goto err;
    }

    // source_clir
    if (!cdr_parse_json_get_int(json, "clir", &cdr->source_clir, 0, 1)) {
        L_ERROR("Call-Id '%s' does not contain 'clir' key (source account id), '%s'", cdr->call_id, srcleg);
        goto err;
    }

    // call_type
    if (!cdr_parse_json_get_string(json, "s", cdr->call_type)) {
        L_ERROR("Call-Id '%s' does not contain 's' key (call type), '%s'", cdr->call_id, srcleg);
        goto err;
    }

    // source_ip
    if (!cdr_parse_json_get_string(json, "ip", cdr->source_ip)) {
        L_ERROR("Call-Id '%s' does not contain 'ip' key (source ip), '%s'", cdr->call_id, srcleg);
        goto err;
    }

    // init_time
    if (!cdr_parse_json_get_double(json, "t", &cdr->init_time, 946684800, 4133980799)) {
        L_ERROR("Call-Id '%s' does not contain 's' key (source init time), '%s'", cdr->call_id, srcleg);
        goto err;
    }

    json_object *temp_value;
    // source_gpp
    if(!json_object_object_get_ex(json, "gpp", &temp_value))
    {
        L_ERROR("Call-Id '%s' does not contain 'gpp' key (source gpp list), '%s'", cdr->call_id, srcleg);
        goto err;
    }
    if (!json_object_is_type(temp_value, json_type_array)) {
        L_ERROR("Call-Id '%s' key 'gpp' doesn't contain an array, '%s'", cdr->call_id, srcleg);
        goto err;
    }
    if(json_object_array_length(temp_value) != 10)
    {
        L_ERROR("Call-Id '%s' has only '%d' source gpp fields, they should be 10, '%s'", cdr->call_id, json_object_array_length(temp_value), srcleg);
        goto err;
    }
    int i;
    for(i = 0; i < 10; ++i)
    {
        g_strlcpy(cdr->source_gpp[i], json_object_get_string(json_object_array_get_idx(temp_value, i)), sizeof(cdr->source_gpp[i]));
    }

    // source_lnp_prefix
    if(!json_object_object_get_ex(json, "lnp_p", &temp_value))
    {
        if (strict_leg_tokens) {    // TODO: "Strict acc fields (move to trash otherwise)" What is the meaning of this param??? is it still necessary
            L_ERROR("Call-Id '%s' does not contain 'lnp_p' key (source lnp prefix), '%s'", cdr->call_id, srcleg);
            goto err;
        } else {
            cdr->source_lnp_prefix[0] = '\0';
            cdr->source_user_out[0] = '\0';
            L_WARNING("Call-Id '%s' does not contain 'lnp_p' key, using empty source lnp prefix and source user out '%s'", cdr->call_id, srcleg);
            goto ret;
        }
    }
    g_strlcpy(cdr->source_lnp_prefix, json_object_get_string(temp_value), sizeof(cdr->source_lnp_prefix));

    // source_user_out
    if(!json_object_object_get_ex(json, "cli_out", &temp_value))
    {
        if (strict_leg_tokens) {    // TODO: "Strict acc fields (move to trash otherwise)" What is the meaning of this param??? is it still necessary
            L_ERROR("Call-Id '%s' does not contain 'cli_out' key (source user out), '%s'", cdr->call_id, srcleg);
            goto err;
        } else {
            cdr->source_user_out[0] = '\0';
            L_WARNING("Call-Id '%s' does not contain 'cli_out' key, using empty source user out '%s'", cdr->call_id, srcleg);
            goto ret;
        }
    }
    g_strlcpy(cdr->source_user_out, json_object_get_string(temp_value), sizeof(cdr->source_user_out));
    uri_unescape(cdr->source_user_out);

    // source_lnp_type
    if (!cdr_parse_json_get_string(json, "lnp_t", cdr->source_lnp_type)) {
        L_ERROR("Call-Id '%s' does not contain 'lnp_t' key (source lnp type), '%s'", cdr->call_id, srcleg);
        goto err;
    }

    // header_pai
    if (!cdr_parse_json_get_string(json, "pai", cdr->header_pai)) {
        L_ERROR("Call-Id '%s' does not contain 'pai' key (P-Asserted-Identity header), '%s'", cdr->call_id, srcleg);
        goto err;
    }

    // header_diversion
    if (!cdr_parse_json_get_string(json, "div", cdr->header_diversion)) {
        L_ERROR("Call-Id '%s' does not contain 'div' key (Diversion header), '%s'", cdr->call_id, srcleg);
        goto err;
    }

    // group
    if (!cdr_parse_json_get_string(json, "cid", cdr->group)) {
        L_ERROR("Call-Id '%s' does not contain 'cid' key (group info), '%s'", cdr->call_id, srcleg);
        goto err;
    }

    // header_u2u
    if (!cdr_parse_json_get_string(json, "u2u", cdr->header_u2u)) {
        L_ERROR("Call-Id '%s' does not contain 'u2u' key (User-to-User header), '%s'", cdr->call_id, srcleg);
        goto err;
    }

    // source_lcr_id
    if (!cdr_parse_json_get_int(json, "lcr", &cdr->source_lcr_id, 0, INT_MAX)) {
        L_DEBUG("Call-Id '%s' does not contain 'lcr' key (source lcr id), '%s'", cdr->call_id, srcleg);
        /// Simply return 0 in order to avoid issues with ACC records in the OLD format during an upgrade
        /// Added in mr8.1, it should be changed to -1 in mr9.+
        /// goto err;
        goto ret;
    }

ret:
    json_object_put(json);
    return 0;

err:
    json_object_put(json);
    return -1;
}

static int cdr_parse_dstleg(char *dstleg, cdr_entry_t *cdr)
{
    L_DEBUG("Parsing JSON: '%s'", dstleg);

    json_object *json = json_tokener_parse(dstleg);
    if (!json) {
        L_ERROR("Could not parse JSON dst_leg string: '%s'", dstleg);
        return -1;
    }
    if (!json_object_is_type(json, json_type_object)) {
        L_ERROR("JSON type is not object: '%s'", dstleg);
        goto err;
    }

    // split
    if (!cdr_parse_json_get_int(json, "plu", &cdr->split, 0, INT_MAX)) {
        L_ERROR("Call-Id '%s' does not contain 'plu' key (split flag), '%s'", cdr->call_id, dstleg);
        goto err;
    }

    // destination_ext_subscriber_id
    if (!cdr_parse_json_get_string(json, "s_id", cdr->destination_ext_subscriber_id)) {
        L_ERROR("Call-Id '%s' does not contain 's_id' key (destination external subscriber id), '%s'", cdr->call_id, dstleg);
        goto err;
    }
    uri_unescape(cdr->destination_ext_subscriber_id);

    // destination_ext_contract_id
    if (!cdr_parse_json_get_string(json, "c_id", cdr->destination_ext_contract_id)) {
        L_ERROR("Call-Id '%s' does not contain 'c_id' key (destination external contract id), '%s'", cdr->call_id, dstleg);
        goto err;
    }
    uri_unescape(cdr->destination_ext_contract_id);

    // destination_account_id
    if (!cdr_parse_json_get_int(json, "a_id", &cdr->destination_account_id, 0, INT_MAX)) {
        L_ERROR("Call-Id '%s' does not contain 'a_id' key (destination account id), '%s'", cdr->call_id, dstleg);
        goto err;
    }

    // destination_dialed
    if (!cdr_parse_json_get_string(json, "dialed", cdr->destination_dialed)) {
        L_ERROR("Call-Id '%s' does not contain 'dialed' key (dialed digit), '%s'", cdr->call_id, dstleg);
        goto err;
    }

    // destination_user_id
    if (!cdr_parse_json_get_string(json, "uuid", cdr->destination_user_id)) {
        L_ERROR("Call-Id '%s' does not contain 'uuid' key (destination user id), '%s'", cdr->call_id, dstleg);
        goto err;
    }

    // destination_user
    if (!cdr_parse_json_get_string(json, "u", cdr->destination_user)) {
        L_ERROR("Call-Id '%s' does not contain 'u' key (destination user), '%s'", cdr->call_id, dstleg);
        goto err;
    }

    // destination_domain
    if (!cdr_parse_json_get_string(json, "d", cdr->destination_domain)) {
        L_ERROR("Call-Id '%s' does not contain 'd' key (destination domain), '%s'", cdr->call_id, dstleg);
        goto err;
    }

    // destination_user_in
    if (!cdr_parse_json_get_string(json, "u_in", cdr->destination_user_in)) {
        L_ERROR("Call-Id '%s' does not contain 'u_in' key (incoming destination user), '%s'", cdr->call_id, dstleg);
        goto err;
    }

    // destination_domain_in
    if (!cdr_parse_json_get_string(json, "d_in", cdr->destination_domain_in)) {
        L_ERROR("Call-Id '%s' does not contain 'd_in' key (incoming destination domain), '%s'", cdr->call_id, dstleg);
        goto err;
    }

    // destination_lcr_id
    if (!cdr_parse_json_get_int(json, "lcr", &cdr->destination_lcr_id, 0, INT_MAX)) {
        L_ERROR("Call-Id '%s' does not contain 'lcr' key (lcr id), '%s'", cdr->call_id, dstleg);
        goto err;
    }

    json_object *temp_value;
    // destination_gpp
    if(!json_object_object_get_ex(json, "gpp", &temp_value))
    {
        L_ERROR("Call-Id '%s' does not contain 'gpp' key (source gpp list), '%s'", cdr->call_id, dstleg);
        goto err;
    }
    if (!json_object_is_type(temp_value, json_type_array)) {
        L_ERROR("Call-Id '%s' key 'gpp' doesn't contain an array, '%s'", cdr->call_id, dstleg);
        goto err;
    }
    if(json_object_array_length(temp_value) != 10)
    {
        L_ERROR("Call-Id '%s' has only '%d' destination gpp fields, they should be 10, '%s'", cdr->call_id, json_object_array_length(temp_value), dstleg);
        goto err;
    }
    int i;
    for(i = 0; i < 10; ++i)
    {
        g_strlcpy(cdr->destination_gpp[i], json_object_get_string(json_object_array_get_idx(temp_value, i)), sizeof(cdr->destination_gpp[i]));
    }

    // destination_lnp_prefix
    if(!json_object_object_get_ex(json, "lnp_p", &temp_value))
    {
        if (strict_leg_tokens) {    // TODO: "Strict acc fields (move to trash otherwise)" What is the meaning of this param??? is it still necessary
            L_ERROR("Call-Id '%s' does not contain 'lnp_p' key (destination lnp prefix), '%s'", cdr->call_id, dstleg);
            goto err;
        } else {
            cdr->destination_lnp_prefix[0] = '\0';
            cdr->destination_user_out[0] = '\0';
            L_WARNING("Call-Id '%s' does not contain 'lnp_p' key, using empty destination lnp prefix and destination user out '%s'", cdr->call_id, dstleg);
            goto ret;
        }
    }
    g_strlcpy(cdr->destination_lnp_prefix, json_object_get_string(temp_value), sizeof(cdr->destination_lnp_prefix));

    // destination_user_out
    if(!json_object_object_get_ex(json, "u_out", &temp_value))
    {
        if (strict_leg_tokens) {    // TODO: "Strict acc fields (move to trash otherwise)" What is the meaning of this param??? is it still necessary
            L_ERROR("Call-Id '%s' does not contain 'cli_out' key (source user out), '%s'", cdr->call_id, dstleg);
            goto err;
        } else {
            cdr->destination_user_out[0] = '\0';
            L_WARNING("Call-Id '%s' does not contain 'cli_out' key, using empty destination user out '%s'", cdr->call_id, dstleg);
            goto ret;
        }
    }
    g_strlcpy(cdr->destination_user_out, json_object_get_string(temp_value), sizeof(cdr->destination_user_out));
    uri_unescape(cdr->destination_user_out);

    // destination_lnp_type
    if (!cdr_parse_json_get_string(json, "lnp_t", cdr->destination_lnp_type)) {
        L_ERROR("Call-Id '%s' does not contain 'lnp_t' key (destination lnp type), '%s'", cdr->call_id, dstleg);
        goto err;
    }

    // furnished_charging_info
    if (!cdr_parse_json_get_string(json, "fci", cdr->furnished_charging_info)) {
        L_ERROR("Call-Id '%s' does not contain 'fci' key (furnished charging info), '%s'", cdr->call_id, dstleg);
        goto err;
    }

ret:
    json_object_put(json);
    return 0;

err:
    json_object_put(json);
    return -1;
}

static int cdr_parse_bye_dstleg(char *dstleg, mos_data_t *mos_data) {
    L_DEBUG("Parsing JSON: '%s'", dstleg);

    json_object *json = json_tokener_parse(dstleg);
    if (!json) {
        L_ERROR("Could not parse JSON dst_leg string: '%s'", dstleg);
        return -1;
    }
    if (!json_object_is_type(json, json_type_object)) {
        L_ERROR("JSON type is not object: '%s'", dstleg);
        goto err;
    }
    json_object *mos;
    if (!json_object_object_get_ex(json, "mos", &mos)
            || !json_object_is_type(mos, json_type_object))
    {
        L_ERROR("JSON object does not contain 'mos' key: '%s'", dstleg);
        goto err;
    }
    if (!cdr_parse_json_get_double(mos, "avg_score", &mos_data->avg_score, 0, 99)) {
        L_ERROR("JSON object does not contain 'mos.avg_score' key: '%s'", dstleg);
        goto err;
    }
    if (!cdr_parse_json_get_int(mos, "avg_packetloss", &mos_data->avg_packetloss, 0, 100)) {
        L_ERROR("JSON object does not contain 'mos.avg_packetloss' key: '%s'", dstleg);
        goto err;
    }
    if (!cdr_parse_json_get_int(mos, "avg_jitter", &mos_data->avg_jitter, 0, 9999)) {
        L_ERROR("JSON object does not contain 'mos.avg_jitter' key: '%s'", dstleg);
        goto err;
    }
    if (!cdr_parse_json_get_int(mos, "avg_rtt", &mos_data->avg_rtt, 0, 9999)) {
        L_ERROR("JSON object does not contain 'mos.avg_rtt' key: '%s'", dstleg);
        goto err;
    }

    mos_data->filled = 1;
    json_object_put(json);
    return 0;

err:
    json_object_put(json);
    return -1;
}


static int cdr_create_cdrs(med_entry_t *records, uint64_t count,
        cdr_entry_t **cdrs, uint64_t *cdr_count, uint8_t *trash, int do_intermediate)
{
    uint64_t i = 0, cdr_index = 0;
    uint32_t invites = 0;
    size_t cdr_size;
    int timed_out = 0;

    char *endtime = NULL;
    double unix_endtime = 0, tmp_unix_endtime = 0;
    const char *call_status;
    mos_data_t mos_data;

    *cdr_count = 0;
    memset(&mos_data, 0, sizeof(mos_data));


    /* get end time from BYE's timestamp */
    for(i = 0; i < count; ++i)
    {
        med_entry_t *e = &(records[i]);
        if (e->timed_out)
            timed_out = 1;

        if(e->valid && e->method == MED_INVITE)
        {
            ++invites;
        }
        else if(e->method == MED_BYE)
        {
            if (endtime == NULL) {
                endtime = e->timestamp;
                unix_endtime = e->unix_timestamp;
            }
            if (!mos_data.filled)
                cdr_parse_bye_dstleg(e->dst_leg, &mos_data);
        }

        if (check_shutdown())
            return -1;
    }

    if(invites == 0)
    {
        L_CRITICAL("No valid INVITEs for creating a cdr, internal error, callid='%s'",
                records[0].callid);
        return -1;
    }

    /* each INVITE maps to a CDR */
    cdr_size = sizeof(cdr_entry_t) * invites;
    *cdrs = (cdr_entry_t*)malloc(cdr_size);
    if(*cdrs == NULL)
    {
        L_ERROR("Error allocating memory for cdrs: %s", strerror(errno));
        return -1;
    }
    memset(*cdrs, 0, cdr_size);

    for(i = 0; i < count; ++i)
    {
        med_entry_t *e = NULL;
        cdr_entry_t *cdr = NULL;


        cdr = &(*cdrs)[cdr_index];
        e = &(records[i]);

        if (!e->valid)
            continue;

        L_DEBUG("create cdr %lu of %lu in batch\n", i, count);

        call_status = cdr_map_status(e->sip_code);
        if (timed_out)
            call_status = CDR_STATUS_FAILED;

        if(e->method == MED_INVITE && call_status != NULL)
        {
            ++cdr_index;

            L_DEBUG("Creating CDR index %lu\n", cdr_index);

            if(strncmp("200", e->sip_code, 3))
            {
                /* missed calls have duration of 0 */
                tmp_unix_endtime = e->unix_timestamp;
            }
            else
            {
                tmp_unix_endtime = unix_endtime;
                if (!tmp_unix_endtime) {
                    if (do_intermediate && !timed_out) {
                        L_DEBUG("CDR %lu is an intermediate record\n", cdr_index);
                        cdr->intermediate = 1;
                    }
                    else {
                        L_DEBUG("CDR %lu is an expired record\n", cdr_index);
                    }
                    tmp_unix_endtime = time(NULL);
                }
            }

            g_strlcpy(cdr->call_id, e->callid, sizeof(cdr->call_id));
            /* g_strlcpy(cdr->start_time, e->timestamp, sizeof(cdr->start_time)); */
            cdr->start_time = e->unix_timestamp;
            cdr->duration = (tmp_unix_endtime >= e->unix_timestamp) ? tmp_unix_endtime - e->unix_timestamp : 0;
            g_strlcpy(cdr->call_status, call_status, sizeof(cdr->call_status));
            g_strlcpy(cdr->call_code, e->sip_code, sizeof(cdr->call_code));


            cdr->source_carrier_cost = 0;
            cdr->source_reseller_cost = 0;
            cdr->source_customer_cost = 0;
            cdr->destination_carrier_cost = 0;
            cdr->destination_reseller_cost = 0;
            cdr->destination_customer_cost = 0;

            if(cdr_parse_srcleg(e->src_leg, cdr) < 0 && !cdr->intermediate)
            {
                *trash = 1;
                return 0;
            }

            if(cdr_parse_dstleg(e->dst_leg, cdr) < 0 && !cdr->intermediate && !timed_out)
            {
                *trash = 1;
                return 0;
            }

            if(cdr_fill_record(cdr) != 0)
            {
                // TODO: error handling
            }

            cdr->mos = mos_data;
            g_strlcpy(cdr->acc_ref, e->acc_ref, sizeof(cdr->acc_ref));

            L_DEBUG("Created CDR index %lu\n", cdr_index);
        }

        if (check_shutdown())
            return -1;
    }

    *cdr_count = cdr_index;

    /*L_DEBUG("Created %llu CDRs:", *cdr_count);*/

    return 0;
}

int cdr_fill_record(cdr_entry_t *cdr)
{
    cdr_set_provider(cdr);

    /*
    if(cdr->source_clir)
    {
        strcpy(cdr->source_cli, "anonymous");
    }
    */

    return 0;
}

void cdr_set_provider(cdr_entry_t *cdr)
{
    char *val;

    if(strncmp("0", cdr->source_user_id, sizeof(cdr->source_user_id)) != 0)
    {
        if((val = g_hash_table_lookup(med_uuid_table, cdr->source_user_id)) != NULL)
        {
            g_strlcpy(cdr->source_provider_id, val, sizeof(cdr->source_provider_id));
        }
        else
        {
            g_strlcpy(cdr->source_provider_id, "0", sizeof(cdr->source_provider_id));
        }
    }
    else if (cdr->source_lcr_id) {
        snprintf(cdr->source_provider_id, sizeof(cdr->source_provider_id),
                "%llu", (unsigned long long) cdr->source_lcr_id);
        val = g_hash_table_lookup(med_peer_id_table, cdr->source_provider_id);
        g_strlcpy(cdr->source_provider_id, val ? : "0", sizeof(cdr->source_provider_id));
    }
    else if((val = g_hash_table_lookup(med_peer_ip_table, cdr->source_domain)) != NULL)
    {
        g_strlcpy(cdr->source_provider_id, val, sizeof(cdr->source_provider_id));
    }
    else if((val = g_hash_table_lookup(med_peer_host_table, cdr->source_domain)) != NULL)
    {
        g_strlcpy(cdr->source_provider_id, val, sizeof(cdr->source_provider_id));
    }
    else
    {
        g_strlcpy(cdr->source_provider_id, "0", sizeof(cdr->source_provider_id));
    }

    if(strncmp("0", cdr->destination_user_id, sizeof(cdr->destination_user_id)) != 0)
    {
        if((val = g_hash_table_lookup(med_uuid_table, cdr->destination_user_id)) != NULL)
        {
            g_strlcpy(cdr->destination_provider_id, val, sizeof(cdr->destination_provider_id));
        }
        else
        {
            g_strlcpy(cdr->destination_provider_id, "0", sizeof(cdr->destination_provider_id));
        }
    }
    else if (cdr->destination_lcr_id) {
        snprintf(cdr->destination_provider_id, sizeof(cdr->destination_provider_id),
                "%llu", (unsigned long long) cdr->destination_lcr_id);
        val = g_hash_table_lookup(med_peer_id_table, cdr->destination_provider_id);
        g_strlcpy(cdr->destination_provider_id, val ? : "0", sizeof(cdr->destination_provider_id));
    }
    else if((val = g_hash_table_lookup(med_peer_ip_table, cdr->destination_domain)) != NULL)
    {
        g_strlcpy(cdr->destination_provider_id, val, sizeof(cdr->destination_provider_id));
    }
    else if((val = g_hash_table_lookup(med_peer_host_table, cdr->destination_domain)) != NULL)
    {
        g_strlcpy(cdr->destination_provider_id, val, sizeof(cdr->destination_provider_id));
    }
    else
    {
        g_strlcpy(cdr->destination_provider_id, "0", sizeof(cdr->destination_provider_id));
    }
}
