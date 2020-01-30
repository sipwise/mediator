#include <ctype.h>
#include <json.h>

#include "cdr.h"
#include "medmysql.h"
#include "medredis.h"
#include "config.h"
#include "mediator.h"
#include "json-c/json.h"

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

static int cdr_parse_srcleg(char *srcleg, cdr_entry_t *cdr)
{
    struct json_object *parsed_json;
    struct json_object *temp_value;

    parsed_json = json_tokener_parse(srcleg);

    // source_user_id
    if(!json_object_object_get_ex(parsed_json, "uuid", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no source user id (uuid), '%s'", cdr->call_id, srcleg);
        return -1;
    }
    g_strlcpy(cdr->source_user_id, json_object_get_string(temp_value), sizeof(cdr->source_user_id));

    // source_user
    if(!json_object_object_get_ex(parsed_json, "u", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated source user (u), '%s'", cdr->call_id, srcleg);
        return -1;
    }
    g_strlcpy(cdr->source_user, json_object_get_string(temp_value), sizeof(cdr->source_user));

    // source_domain
    if(!json_object_object_get_ex(parsed_json, "d", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated source domain (d), '%s'", cdr->call_id, srcleg);
        return -1;
    }
    g_strlcpy(cdr->source_domain, json_object_get_string(temp_value), sizeof(cdr->source_domain));

    // source_cli
    if(!json_object_object_get_ex(parsed_json, "cli", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated source cli (cli), '%s'", cdr->call_id, srcleg);
        return -1;
    }
    g_strlcpy(cdr->source_cli, json_object_get_string(temp_value), sizeof(cdr->source_cli));

    // source_ext_subscriber_id
    if(!json_object_object_get_ex(parsed_json, "s_id", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated source external subscriber id (s_id), '%s'", cdr->call_id, srcleg);
        return -1;
    }
    g_strlcpy(cdr->source_ext_subscriber_id, json_object_get_string(temp_value), sizeof(cdr->source_ext_subscriber_id));
    uri_unescape(cdr->source_ext_subscriber_id);

    // source_ext_contract_id
    if(!json_object_object_get_ex(parsed_json, "c_id", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated source external contract id (c_id), '%s'", cdr->call_id, srcleg);
        return -1;
    }
    g_strlcpy(cdr->source_ext_contract_id, json_object_get_string(temp_value), sizeof(cdr->source_ext_contract_id));
    uri_unescape(cdr->source_ext_contract_id);

    // source_account_id
    if(!json_object_object_get_ex(parsed_json, "a_id", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated source account id (a_id), '%s'", cdr->call_id, srcleg);
        return -1;
    }
    cdr->source_account_id = json_object_get_int(temp_value);

    // peer_auth_user
    if(!json_object_object_get_ex(parsed_json, "pau", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated peer auth user (pau), '%s'", cdr->call_id, srcleg);
        return -1;
    }
    g_strlcpy(cdr->peer_auth_user, json_object_get_string(temp_value), sizeof(cdr->peer_auth_user));

    // peer_auth_realm
    if(!json_object_object_get_ex(parsed_json, "par", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated peer auth realm (par), '%s'", cdr->call_id, srcleg);
        return -1;
    }
    g_strlcpy(cdr->peer_auth_realm, json_object_get_string(temp_value), sizeof(cdr->peer_auth_realm));

    // source_clir
    if(!json_object_object_get_ex(parsed_json, "clir", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated source clir status (clir), '%s'", cdr->call_id, srcleg);
        return -1;
    }
    cdr->source_clir = json_object_get_int(temp_value);

    // call_type
    if(!json_object_object_get_ex(parsed_json, "s", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated call type (s), '%s'", cdr->call_id, srcleg);
        return -1;
    }
    g_strlcpy(cdr->call_type, json_object_get_string(temp_value), sizeof(cdr->call_type));

    // source_ip
    if(!json_object_object_get_ex(parsed_json, "ip", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated source ip (ip), '%s'", cdr->call_id, srcleg);
        return -1;
    }
    g_strlcpy(cdr->source_ip, json_object_get_string(temp_value), sizeof(cdr->source_ip));

    // init_time
    if(!json_object_object_get_ex(parsed_json, "t", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated source init time (t), '%s'", cdr->call_id, srcleg);
        return -1;
    }
    cdr->init_time = json_object_get_double(temp_value);

    // source_gpp
    if(!json_object_object_get_ex(parsed_json, "gpp", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated source gpp list (gpp), '%s'", cdr->call_id, srcleg);
        return -1;
    }
    if(json_object_array_length(temp_value) != 10)
    {
        L_WARNING("Call-Id '%s' has only '%d' source gpp fields, they should be 10, '%s'", cdr->call_id, json_object_array_length(temp_value), srcleg);
        return -1;
    }    
    int i;
    for(i = 0; i < 10; ++i)
    {
        g_strlcpy(cdr->source_gpp[i], json_object_get_string(json_object_array_get_idx(temp_value, i)), sizeof(cdr->source_gpp[i]));
    }

    // source_lnp_prefix
    if(!json_object_object_get_ex(parsed_json, "lnp_p", &temp_value))
    {
        if (strict_leg_tokens) {
            L_WARNING("Call-Id '%s' has no separated source lnp prefix (lnp_p), '%s'", cdr->call_id, srcleg);
            return -1;
        } else {
            cdr->source_lnp_prefix[0] = '\0';
            cdr->source_user_out[0] = '\0';
            L_WARNING("Call-Id '%s' src leg has missing tokens (using empty lnp prefix, user out) '%s'", cdr->call_id, srcleg);
            return 0;
        }
    }
    g_strlcpy(cdr->source_lnp_prefix, json_object_get_string(temp_value), sizeof(cdr->source_lnp_prefix));

    // source_user_out
    if(!json_object_object_get_ex(parsed_json, "cli_out", &temp_value))
    {
        if (strict_leg_tokens) {
            L_WARNING("Call-Id '%s' has no separated source user out (cli_out), '%s'", cdr->call_id, srcleg);
            return -1;
        } else {
            cdr->source_user_out[0] = '\0';
            L_WARNING("Call-Id '%s' src leg has missing tokens (using empty user out) '%s'", cdr->call_id, srcleg);
            return 0;
        }
    }
    g_strlcpy(cdr->source_user_out, json_object_get_string(temp_value), sizeof(cdr->source_user_out));
    uri_unescape(cdr->source_user_out);

    // source_lnp_type
    if(!json_object_object_get_ex(parsed_json, "lnp_t", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated source lnp type (lnp_t), '%s'", cdr->call_id, srcleg);
        return -1;
    }
    g_strlcpy(cdr->source_lnp_type, json_object_get_string(temp_value), sizeof(cdr->source_lnp_type));

    // header_pai
    if(!json_object_object_get_ex(parsed_json, "pai", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated P-Asserted-Identity header (pai), '%s'", cdr->call_id, srcleg);
        return -1;
    }
    g_strlcpy(cdr->header_pai, json_object_get_string(temp_value), sizeof(cdr->header_pai));

    // header_diversion
    if(!json_object_object_get_ex(parsed_json, "div", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated source Diversion header (div), '%s'", cdr->call_id, srcleg);
        return -1;
    }
    g_strlcpy(cdr->header_diversion, json_object_get_string(temp_value), sizeof(cdr->header_diversion));

    // group
    if(!json_object_object_get_ex(parsed_json, "cid", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated group info (cid), '%s'", cdr->call_id, srcleg);
        return -1;
    }
    g_strlcpy(cdr->group, json_object_get_string(temp_value), sizeof(cdr->group));

    // header_u2u
    if(!json_object_object_get_ex(parsed_json, "u2u", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated User-to-User header (u2u), '%s'", cdr->call_id, srcleg);
        return -1;
    }
    g_strlcpy(cdr->header_u2u, json_object_get_string(temp_value), sizeof(cdr->header_u2u));

    // source_lcr_id
    if(!json_object_object_get_ex(parsed_json, "lcr", &temp_value))
    {
        L_DEBUG("Call-Id '%s' has no separated source lcr id (lcr), '%s'", cdr->call_id, srcleg);
        /// Simply return 0 in order to avoid issues with ACC records in the OLD format during an upgrade
        /// Added in mr8.1, it should be changed to -1 in mr9.+
        return 0;
    }
    cdr->source_lcr_id = json_object_get_int(temp_value);

    return 0;
}

static int cdr_parse_dstleg(char *dstleg, cdr_entry_t *cdr)
{
    struct json_object *parsed_json;
    struct json_object *temp_value;

    parsed_json = json_tokener_parse(dstleg);

    // split
    if(!json_object_object_get_ex(parsed_json, "plu", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated split flag (plu), '%s'", cdr->call_id, dstleg);
        return -1;
    }
    cdr->split = json_object_get_int(temp_value);

    // destination_ext_subscriber_id
    if(!json_object_object_get_ex(parsed_json, "s_id", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated destination external subscriber id (s_id), '%s'", cdr->call_id, dstleg);
        return -1;
    }
    g_strlcpy(cdr->destination_ext_subscriber_id, json_object_get_string(temp_value), sizeof(cdr->destination_ext_subscriber_id));
    uri_unescape(cdr->destination_ext_subscriber_id);

    // destination_ext_contract_id
    if(!json_object_object_get_ex(parsed_json, "c_id", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated destination external contract id (c_id), '%s'", cdr->call_id, dstleg);
        return -1;
    }
    g_strlcpy(cdr->destination_ext_contract_id, json_object_get_string(temp_value), sizeof(cdr->destination_ext_contract_id));
    uri_unescape(cdr->destination_ext_contract_id);

    // destination_account_id
    if(!json_object_object_get_ex(parsed_json, "a_id", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated destination account id (a_id), '%s'", cdr->call_id, dstleg);
        return -1;
    }
    cdr->destination_account_id = json_object_get_int(temp_value);

    // destination_dialed
    if(!json_object_object_get_ex(parsed_json, "dialed", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated dialed digits (dialed), '%s'", cdr->call_id, dstleg);
        return -1;
    }
    g_strlcpy(cdr->destination_dialed, json_object_get_string(temp_value), sizeof(cdr->destination_dialed));

    // destination_user_id
    if(!json_object_object_get_ex(parsed_json, "uuid", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated destination user id (uuid), '%s'", cdr->call_id, dstleg);
        return -1;
    }
    g_strlcpy(cdr->destination_user_id, json_object_get_string(temp_value), sizeof(cdr->destination_user_id));

    // destination_user
    if(!json_object_object_get_ex(parsed_json, "u", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated destination user (u), '%s'", cdr->call_id, dstleg);
        return -1;
    }
    g_strlcpy(cdr->destination_user, json_object_get_string(temp_value), sizeof(cdr->destination_user));

    // destination_domain
    if(!json_object_object_get_ex(parsed_json, "d", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated destination domain (d), '%s'", cdr->call_id, dstleg);
        return -1;
    }
    g_strlcpy(cdr->destination_domain, json_object_get_string(temp_value), sizeof(cdr->destination_domain));

    // destination_user_in
    if(!json_object_object_get_ex(parsed_json, "u_in", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated incoming destination user (u_in), '%s'", cdr->call_id, dstleg);
        return -1;
    }
    g_strlcpy(cdr->destination_user_in, json_object_get_string(temp_value), sizeof(cdr->destination_user_in));

    // destination_domain_in
    if(!json_object_object_get_ex(parsed_json, "d_in", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated incoming destination domain (d_in), '%s'", cdr->call_id, dstleg);
        return -1;
    }
    g_strlcpy(cdr->destination_domain_in, json_object_get_string(temp_value), sizeof(cdr->destination_domain_in));

    // destination_lcr_id
    if(!json_object_object_get_ex(parsed_json, "lcr", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated destination lcr id (lcr), '%s'", cdr->call_id, dstleg);
        return -1;
    }
    cdr->destination_lcr_id = json_object_get_int(temp_value);

    // destination_gpp
    if(!json_object_object_get_ex(parsed_json, "gpp", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated destination gpp list (gpp), '%s'", cdr->call_id, dstleg);
        return -1;
    }
    if(json_object_array_length(temp_value) != 10)
    {
        L_WARNING("Call-Id '%s' has only '%d' destination gpp fields, they should be 10, '%s'", cdr->call_id, json_object_array_length(temp_value), dstleg);
        return -1;
    }    
    int i;
    for(i = 0; i < 10; ++i)
    {
        g_strlcpy(cdr->destination_gpp[i], json_object_get_string(json_object_array_get_idx(temp_value, i)), sizeof(cdr->destination_gpp[i]));
    }

    // destination_lnp_prefix
    if(!json_object_object_get_ex(parsed_json, "lnp_p", &temp_value))
    {
        if (strict_leg_tokens) {
            L_WARNING("Call-Id '%s' has no separated destination lnp prefix (lnp_p), '%s'", cdr->call_id, dstleg);
            return -1;
        } else {
            cdr->destination_lnp_prefix[0] = '\0';
            cdr->destination_user_out[0] = '\0';
            L_WARNING("Call-Id '%s' dst leg has missing tokens (using empty lnp prefix, user out) '%s'", cdr->call_id, dstleg);
            return 0;
        }
    }
    g_strlcpy(cdr->destination_lnp_prefix, json_object_get_string(temp_value), sizeof(cdr->destination_lnp_prefix));

    // destination_user_out
    if(!json_object_object_get_ex(parsed_json, "u_out", &temp_value))
    {
        if (strict_leg_tokens) {
            L_WARNING("Call-Id '%s' has no separated destination user out (u_out), '%s'", cdr->call_id, dstleg);
            return -1;
        } else {
            cdr->destination_user_out[0] = '\0';
            L_WARNING("Call-Id '%s' dst leg has missing tokens (using empty user out) '%s'", cdr->call_id, dstleg);
            return 0;
        }
    }
    g_strlcpy(cdr->destination_user_out, json_object_get_string(temp_value), sizeof(cdr->destination_user_out));
    uri_unescape(cdr->destination_user_out);

    // destination_lnp_type
    if(!json_object_object_get_ex(parsed_json, "lnp_t", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated destination lnp type (lnp_t), '%s'", cdr->call_id, dstleg);
        return -1;
    }
    g_strlcpy(cdr->destination_lnp_type, json_object_get_string(temp_value), sizeof(cdr->destination_lnp_type));

    // furnished_charging_info
    if(!json_object_object_get_ex(parsed_json, "fci", &temp_value))
    {
        L_WARNING("Call-Id '%s' has no separated furnished charging info (fci), '%s'", cdr->call_id, dstleg);
        return -1;
    }
    g_strlcpy(cdr->furnished_charging_info, json_object_get_string(temp_value), sizeof(cdr->furnished_charging_info));

    return 0;
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
