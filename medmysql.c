#include <stdbool.h>
#include <mysql.h>
#ifdef MARIADB
#include <mariadb/errmsg.h>
#include <mariadb/mysqld_error.h>
#else
#include <mysql/errmsg.h>
#include <mysql/mysqld_error.h>
#endif
#include <assert.h>

#include "medmysql.h"
#include "config.h"
#include "records.h"

#define _TEST_SIMULATE_SQL_ERRORS 0

#define MED_CACHE_DURATION 30

#define MED_SQL_BUF_LEN(fixed_string_len, escaped_string_len) \
    (fixed_string_len + 7 + 2 + (escaped_string_len) * 2 + 1) // _latin1'STRING'\0

#define MED_CALLID_QUERY "select a.callid from acc a" \
    " where a.method = 'INVITE' " \
   " group by a.callid limit 0,200000"

#define MED_FETCH_QUERY "(select distinct sip_code, sip_reason, method, callid, time, time_hires, " \
    "src_leg, dst_leg, branch_id, id " \
    "from acc where method = 'INVITE' and callid = '%s' order by time_hires asc) " \
    "union all " \
    "(select distinct sip_code, sip_reason, method, callid, time, time_hires, " \
    "src_leg, dst_leg, branch_id, id " \
    "from acc where method = 'BYE' and callid in ('%s', '%s"PBXSUFFIX"') " \
    "order by length(callid) asc, time_hires asc) " \
    "union all " \
    "(select distinct sip_code, sip_reason, method, callid, time, time_hires, " \
    "src_leg, dst_leg, branch_id, id " \
    "from acc where method = 'BYE' and callid in ('%s', '%s"XFERSUFFIX"') " \
    "order by length(callid) asc, time_hires asc) " \
    "union all " \
    "(select distinct sip_code, sip_reason, method, callid, time, time_hires, " \
    "src_leg, dst_leg, branch_id, id " \
    "from acc where method = 'BYE' and callid in ('%s', '%s"PBXSUFFIX""XFERSUFFIX"') " \
    "order by length(callid) asc, time_hires asc)"

#define MED_LOAD_PEER_QUERY "select h.ip, h.host, g.peering_contract_id, h.id " \
    "from provisioning.voip_peer_hosts h, provisioning.voip_peer_groups g " \
    "where g.id = h.group_id"

#define MED_LOAD_CDR_TAG_IDS_QUERY "select id, type from accounting.cdr_tag"

typedef struct _medmysql_handler {
    const char *name;
    MYSQL *m;
    int is_transaction;
    GQueue transaction_statements;
} medmysql_handler;
typedef struct _statement_str {
    char *str;
    unsigned long len;
} statement_str;
typedef struct _cdr_tag_record {
    unsigned long long cdr_id;
    char *sql_record;
} cdr_tag_record;
typedef struct {
    const char *begin;
    const char *end;
    unsigned long cdr_id;
} single_cdr;

typedef struct _medmysql_batch_definition {
    const char *sql_init_string,
          *sql_finish_string;
    unsigned int min_string_tail_room; // defaults to 1024 if not set
    int (*single_flush_func)(struct medmysql_str *);
    int (*full_flush_func)(struct medmysql_batches *);
    medmysql_handler **handler_ptr;
} medmysql_batch_definition;

static medmysql_handler *cdr_handler;
static medmysql_handler *int_cdr_handler;
static medmysql_handler *med_handler;
static medmysql_handler *prov_handler;
static medmysql_handler *stats_handler;

static unsigned long medmysql_cdr_auto_increment_value;
static unsigned long medmysql_tag_provider_customer;
static unsigned long medmysql_tag_provider_reseller;
static unsigned long medmysql_tag_provider_carrier;
static unsigned long medmysql_tag_direction_source;
static unsigned long medmysql_tag_direction_destination;

static int medmysql_flush_cdr(struct medmysql_batches *);
static int medmysql_flush_int_cdr(struct medmysql_batches *);
static int medmysql_flush_all_med(struct medmysql_batches *);
static int medmysql_flush_med_str(struct medmysql_str *);
static int medmysql_flush_medlist(struct medmysql_str *);
static int medmysql_flush_call_stat_info(void);
static void medmysql_handler_close(medmysql_handler **h);
static int medmysql_handler_transaction(medmysql_handler *h);


static const medmysql_batch_definition medmysql_trash_def = {
    .sql_init_string = "insert into acc_trash (method, from_tag, to_tag, callid, sip_code, " \
                "sip_reason, time, time_hires, src_leg, dst_leg, dst_user, dst_ouser, " \
                "dst_domain, src_user, src_domain, branch_id) select method, from_tag, to_tag, " \
                "callid, sip_code, sip_reason, time, time_hires, src_leg, dst_leg, " \
                "dst_user, dst_ouser, dst_domain, src_user, src_domain, branch_id from acc " \
                "where callid in (",
    .sql_finish_string = ")",
    .single_flush_func = medmysql_flush_medlist,
    .handler_ptr = &med_handler,
};
static const medmysql_batch_definition medmysql_backup_def = {
    .sql_init_string = "insert into acc_backup (method, from_tag, to_tag, callid, sip_code, " \
                "sip_reason, time, time_hires, src_leg, dst_leg, dst_user, dst_ouser, " \
                "dst_domain, src_user, src_domain, branch_id) select method, from_tag, to_tag, " \
                "callid, sip_code, sip_reason, time, time_hires, src_leg, dst_leg, " \
                "dst_user, dst_ouser, dst_domain, src_user, src_domain, branch_id from acc " \
                "where callid in (",
    .sql_finish_string = ")",
    .single_flush_func = medmysql_flush_medlist,
    .handler_ptr = &med_handler,
};
static const medmysql_batch_definition medmysql_delete_def = {
    .sql_init_string = "delete from acc where callid in (",
    .sql_finish_string = ")",
    .full_flush_func = medmysql_flush_all_med,
    .handler_ptr = &med_handler,
};
static const medmysql_batch_definition medmysql_cdr_def = {
    .sql_init_string = "insert into cdr (id, update_time, " \
        "source_user_id, source_provider_id, source_external_subscriber_id, "\
        "source_external_contract_id, source_account_id, source_user, source_domain, " \
        "source_cli, source_clir, source_ip, "\
        "destination_user_id, destination_provider_id, destination_external_subscriber_id, "\
        "destination_external_contract_id, destination_account_id, destination_user, destination_domain, " \
        "destination_user_in, destination_domain_in, destination_user_dialed, " \
        "peer_auth_user, peer_auth_realm, call_type, call_status, call_code, init_time, start_time, "\
        "duration, call_id, " \
        "source_carrier_cost, source_reseller_cost, source_customer_cost, " \
        "destination_carrier_cost, destination_reseller_cost, destination_customer_cost, " \
        "split, " \
        "source_gpp0, source_gpp1, source_gpp2, source_gpp3, source_gpp4, " \
        "source_gpp5, source_gpp6, source_gpp7, source_gpp8, source_gpp9, " \
        "destination_gpp0, destination_gpp1, destination_gpp2, destination_gpp3, destination_gpp4, " \
        "destination_gpp5, destination_gpp6, destination_gpp7, destination_gpp8, destination_gpp9, " \
        "source_lnp_prefix, destination_lnp_prefix, " \
        "source_user_out, destination_user_out, " \
        "source_lnp_type, destination_lnp_type" \
        ") values ",
    .full_flush_func = medmysql_flush_cdr,
    .min_string_tail_room = 6000,
    .handler_ptr = &cdr_handler,
};
static const medmysql_batch_definition medmysql_int_cdr_def = {
    .sql_init_string = "insert into int_cdr (id, update_time, " \
        "source_user_id, source_provider_id, source_external_subscriber_id, "\
        "source_external_contract_id, source_account_id, source_user, source_domain, " \
        "source_cli, source_clir, source_ip, "\
        "destination_user_id, destination_provider_id, destination_external_subscriber_id, "\
        "destination_external_contract_id, destination_account_id, destination_user, destination_domain, " \
        "destination_user_in, destination_domain_in, destination_user_dialed, " \
        "peer_auth_user, peer_auth_realm, call_type, call_status, call_code, init_time, start_time, "\
        "duration, call_id, " \
        "source_carrier_cost, source_reseller_cost, source_customer_cost, " \
        "destination_carrier_cost, destination_reseller_cost, destination_customer_cost, " \
        "split, " \
        "source_gpp0, source_gpp1, source_gpp2, source_gpp3, source_gpp4, " \
        "source_gpp5, source_gpp6, source_gpp7, source_gpp8, source_gpp9, " \
        "destination_gpp0, destination_gpp1, destination_gpp2, destination_gpp3, destination_gpp4, " \
        "destination_gpp5, destination_gpp6, destination_gpp7, destination_gpp8, destination_gpp9, " \
        "source_lnp_prefix, destination_lnp_prefix, " \
        "source_user_out, destination_user_out, " \
        "source_lnp_type, destination_lnp_type, acc_ref" \
        ") values ",
    .sql_finish_string = " on duplicate key update "
        "update_time = values(update_time), source_user_id = values(source_user_id), " \
        "source_provider_id = values(source_provider_id), source_external_subscriber_id " \
        "= values(source_external_subscriber_id), source_external_contract_id = " \
        "values(source_external_contract_id), source_account_id = " \
        "values(source_account_id), source_user = values(source_user), source_domain = " \
        "values(source_domain), source_cli = values(source_cli), source_clir = " \
        "values(source_clir), source_ip = values(source_ip), destination_user_id = " \
        "values(destination_user_id), destination_provider_id = " \
        "values(destination_provider_id), destination_external_subscriber_id = " \
        "values(destination_external_subscriber_id), destination_external_contract_id = " \
        "values(destination_external_contract_id), destination_account_id = " \
        "values(destination_account_id), destination_user = values(destination_user), " \
        "destination_domain = values(destination_domain), destination_user_in = " \
        "values(destination_user_in), destination_domain_in = " \
        "values(destination_domain_in), destination_user_dialed = " \
        "values(destination_user_dialed), peer_auth_user = values(peer_auth_user), " \
        "peer_auth_realm = values(peer_auth_realm), call_type = values(call_type), " \
        "call_status = values(call_status), call_code = values(call_code), init_time = " \
        "values(init_time), start_time = values(start_time), duration = " \
        "values(duration), call_id = values(call_id), source_carrier_cost = " \
        "values(source_carrier_cost), source_reseller_cost = " \
        "values(source_reseller_cost), source_customer_cost = " \
        "values(source_customer_cost), destination_carrier_cost = " \
        "values(destination_carrier_cost), destination_reseller_cost = " \
        "values(destination_reseller_cost), destination_customer_cost = " \
        "values(destination_customer_cost), split = values(split), source_gpp0 = " \
        "values(source_gpp0), source_gpp1 = values(source_gpp1), source_gpp2 = " \
        "values(source_gpp2), source_gpp3 = values(source_gpp3), source_gpp4 = " \
        "values(source_gpp4), source_gpp5 = values(source_gpp5), source_gpp6 = " \
        "values(source_gpp6), source_gpp7 = values(source_gpp7), source_gpp8 = " \
        "values(source_gpp8), source_gpp9 = values(source_gpp9), destination_gpp0 = " \
        "values(destination_gpp0), destination_gpp1 = values(destination_gpp1), " \
        "destination_gpp2 = values(destination_gpp2), destination_gpp3 = " \
        "values(destination_gpp3), destination_gpp4 = values(destination_gpp4), " \
        "destination_gpp5 = values(destination_gpp5), destination_gpp6 = " \
        "values(destination_gpp6), destination_gpp7 = values(destination_gpp7), " \
        "destination_gpp8 = values(destination_gpp8), destination_gpp9 = " \
        "values(destination_gpp9), source_lnp_prefix = values(source_lnp_prefix), " \
        "destination_lnp_prefix = values(destination_lnp_prefix), source_user_out = " \
        "values(source_user_out), destination_user_out = values(destination_user_out), " \
        "source_lnp_type = values(source_lnp_type), destination_lnp_type = " \
        "values(destination_lnp_type), export_status = 'unexported'",
    .full_flush_func = medmysql_flush_int_cdr,
    .min_string_tail_room = 9000,
    .handler_ptr = &int_cdr_handler,
};
static const medmysql_batch_definition medmysql_del_int_cdr_def = {
    .sql_init_string = "delete from int_cdr where call_id in (",
    .sql_finish_string = ")",
    .single_flush_func = medmysql_flush_med_str,
    .handler_ptr = &int_cdr_handler,
};
static const medmysql_batch_definition medmysql_tag_def = {
    .sql_init_string = "insert into cdr_tag_data (cdr_id, provider_id, direction_id, tag_id, " \
                "val, cdr_start_time) values ",
    .single_flush_func = medmysql_flush_med_str,
    .handler_ptr = &cdr_handler,
};
static const medmysql_batch_definition medmysql_int_tag_def = {
    .sql_init_string = "insert into int_cdr_tag_data (cdr_id, provider_id, direction_id, tag_id, " \
                "val, cdr_start_time) values ",
    .sql_finish_string = " on duplicate key update "
        "val = values(val)",
    .single_flush_func = medmysql_flush_med_str,
    .handler_ptr = &int_cdr_handler,
};
static const medmysql_batch_definition medmysql_mos_def = {
    .sql_init_string = "insert into cdr_mos_data (" \
                "cdr_id, mos_average, mos_average_packetloss, mos_average_jitter, " \
                "mos_average_roundtrip, cdr_start_time" \
                ") values ",
    .single_flush_func = medmysql_flush_med_str,
    .handler_ptr = &cdr_handler,
};
static const medmysql_batch_definition medmysql_group_def = {
    .sql_init_string = "insert into cdr_group (" \
                "cdr_id, call_id, cdr_start_time" \
                ") values ",
    .single_flush_func = medmysql_flush_med_str,
    .handler_ptr = &cdr_handler,
};
static const medmysql_batch_definition medmysql_int_group_def = {
    .sql_init_string = "insert ignore into int_cdr_group (" \
                "cdr_id, call_id, cdr_start_time" \
                ") values ",
    .single_flush_func = medmysql_flush_med_str,
    .handler_ptr = &int_cdr_handler,
};


static void statement_free(void *stm_p) {
    statement_str *stm = stm_p;
    free(stm->str);
    free(stm);
}
static void __g_queue_clear_full(GQueue *q, GDestroyNotify free_func) {
    void *p;
    while ((p = g_queue_pop_head(q)))
        free_func(p);
}


static unsigned int medmysql_real_query_errno(MYSQL *m, const char *s, unsigned long len) {
#if _TEST_SIMULATE_SQL_ERRORS
    if (rand() % 10 == 0) {
        L_INFO("Simulating SQL error - statement '%.*s'",
                (int) len, s);
        return CR_SERVER_LOST;
    }
#endif
    int ret = mysql_real_query(m, s, len);
    if (!ret)
        return 0;
    return mysql_errno(m);
}


static int medmysql_query_wrapper(medmysql_handler *mysql, const char *stmt_str, unsigned long length) {
    int i;
    unsigned int err;

    for (i = 0; i < 10; i++) {
        err = medmysql_real_query_errno(mysql->m, stmt_str, length);
        if (!err)
            break;
        if (err == CR_SERVER_GONE_ERROR || err == CR_SERVER_LOST || err == CR_CONN_HOST_ERROR
                || err == CR_CONNECTION_ERROR)
        {
            L_WARNING("Lost connection to SQL server during query, retrying...");
            sleep(10);
            continue;
        }
        break;
    }
    return !!err;
}


static int medmysql_query_wrapper_tx(medmysql_handler *mysql, const char *stmt_str, unsigned long length) {
    int i;
    unsigned int err;

    if (!mysql->is_transaction) {
        L_CRITICAL("SQL mode is not in transaction");
        return -1;
    }

    for (i = 0; i < 10; i++) {
        err = medmysql_real_query_errno(mysql->m, stmt_str, length);
        if (!err)
            break;
        if (err == CR_SERVER_GONE_ERROR || err == CR_SERVER_LOST || err == CR_CONN_HOST_ERROR
                || err == CR_CONNECTION_ERROR || err == ER_LOCK_WAIT_TIMEOUT
                || err == ER_LOCK_DEADLOCK)
        {
            // rollback, cancel transaction, restart transaction, replay all statements,
            // and then try again
            L_WARNING("Got error %u from SQL server during transaction, retrying...",
                    err);
            err = medmysql_real_query_errno(mysql->m, "rollback", 8);
            if (err) {
                L_CRITICAL("Got error %u from SQL during rollback",
                        mysql_errno(mysql->m));
                return -1;
            }
            mysql->is_transaction = 0;

            sleep(10);

            if (medmysql_handler_transaction(mysql))
                return -1;

            // steal the statement queue and recursively replay them into a new empty queue
            GQueue replay = mysql->transaction_statements;
            g_queue_init(&mysql->transaction_statements);
            statement_str *stm;
            while ((stm = g_queue_pop_head(&replay))) {
                if (medmysql_query_wrapper_tx(mysql, stm->str, stm->len)) {
                    __g_queue_clear_full(&mysql->transaction_statements, statement_free);
                    statement_free(stm);
                    return -1;
                }
                statement_free(stm);
            }

            continue;
        }
        break;
    }
    if (!err) {
        // append statement to queue for possible replaying
        statement_str *stm = malloc(sizeof(*stm));
        if (!stm) {
            L_CRITICAL("Out of memory (malloc statement_str)");
            return -1;
        }
        stm->str = malloc(length);
        if (!stm->str) {
            L_CRITICAL("Out of memory (malloc statement_str body)");
            free(stm);
            return -1;
        }
        memcpy(stm->str, stmt_str, length);
        stm->len = length;
        g_queue_push_tail(&mysql->transaction_statements, stm);
    }
    return !!err;
}

static medmysql_handler *medmysql_handler_init(const char *name, const char *host, const char *user,
        const char *pass, const char *db, unsigned int port)
{
    medmysql_handler *ret;
    bool recon = 1;

    ret = malloc(sizeof(*ret));
    if (!ret) {
        L_CRITICAL("Out of memory (malloc in medmysql_handler_init)");
        return NULL;
    }
    memset(ret, 0, sizeof(*ret));
    ret->name = name;
    g_queue_init(&ret->transaction_statements);
    ret->m = mysql_init(NULL);
    if (!ret->m) {
        L_CRITICAL("Out of memory (mysql_init)");
        goto err;
    }

    if(!mysql_real_connect(ret->m,
                host, user, pass,
                db, port, NULL, 0))
    {
        L_CRITICAL("Error connecting to %s db: %s", name, mysql_error(ret->m));
        goto err;
    }
    if(mysql_options(ret->m, MYSQL_OPT_RECONNECT, &recon) != 0)
    {
        L_CRITICAL("Error setting reconnect-option for %s db: %s", name, mysql_error(ret->m));
        goto err;
    }
    if(mysql_autocommit(ret->m, 1) != 0)
    {
        L_CRITICAL("Error setting autocommit=1 for %s db: %s", name,
                mysql_error(ret->m));
        goto err;
    }

    return ret;

err:
    medmysql_handler_close(&ret);
    return NULL;
}

/**********************************************************************/
static unsigned long medmysql_get_num_col(medmysql_handler *handler, const char *query) {
    if (medmysql_query_wrapper(handler, query, strlen(query))) {
        L_CRITICAL("Error getting DB value (query '%s'): %s",
                query, mysql_error(handler->m));
        return 0;
    }
    MYSQL_RES *res = mysql_store_result(handler->m);
    if (!res) {
        L_CRITICAL("No result set returned from SQL (query '%s'): %s",
                query, mysql_error(handler->m));
        return 0;
    }
    MYSQL_ROW row = mysql_fetch_row(res);
    if (!row || !row[0]) {
        L_CRITICAL("No row returned from SQL (query '%s'): %s",
                query, mysql_error(handler->m));
        mysql_free_result(res);
        return 0;
    }
    unsigned long num = strtoul(row[0], NULL, 10);
    if (!num) {
        L_CRITICAL("Returned number from DB (query '%s') is '%s'",
                query, row[0]);
        mysql_free_result(res);
        return 0;
    }

    mysql_free_result(res);

    return num;
}


/**********************************************************************/
int medmysql_init()
{
    cdr_handler = medmysql_handler_init("CDR",
                config_cdr_host, config_cdr_user, config_cdr_pass,
                config_cdr_db, config_cdr_port);
    if (!cdr_handler)
        goto err;

    int_cdr_handler = medmysql_handler_init("INT-CDR",
                config_intermediate_cdr_host, config_cdr_user, config_cdr_pass,
                config_cdr_db, config_intermediate_cdr_port);
    if (!int_cdr_handler)
        goto err;

    med_handler = medmysql_handler_init("ACC",
                config_med_host, config_med_user, config_med_pass,
                config_med_db, config_med_port);
    if (!med_handler)
        goto err;

    prov_handler = medmysql_handler_init("provisioning",
                config_prov_host, config_prov_user, config_prov_pass,
                config_prov_db, config_prov_port);
    if (!prov_handler)
        goto err;

    stats_handler = medmysql_handler_init("STATS",
                config_stats_host, config_stats_user, config_stats_pass,
                config_stats_db, config_stats_port);
    if (!stats_handler)
        goto err;

    return 0;

err:
    medmysql_cleanup();
    return -1;
}

/**********************************************************************/
static void medmysql_handler_close(medmysql_handler **h) {
    if (!*h)
        return;

    if ((*h)->m)
        mysql_close((*h)->m);

    if ((*h)->transaction_statements.length)
        L_WARNING("Closing DB handle with still %u statements in queue",
                (*h)->transaction_statements.length);
    __g_queue_clear_full(&(*h)->transaction_statements, statement_free);

    free(*h);
    *h = NULL;
}

void medmysql_cleanup()
{
    medmysql_handler_close(&cdr_handler);
    medmysql_handler_close(&int_cdr_handler);
    medmysql_handler_close(&med_handler);
    medmysql_handler_close(&prov_handler);
    medmysql_handler_close(&stats_handler);
}

static void medmysql_buf_escape(MYSQL *m, size_t *buflen, const char *str, const size_t str_len,
		char *orig_dest,
		size_t sql_buffer_size)
{
	char *dest = orig_dest;

	// verify buffer space requirements: string itself * 2, quotes '', optional "_latin1", null
	if (MED_SQL_BUF_LEN(*buflen, str_len) > sql_buffer_size)
		abort();

	if (!g_utf8_validate(str, str_len, NULL)) {
		// force the string as latin1
		memcpy(dest, "_latin1'", 8);
		dest += 8;
	}
	else
		*dest++ = '\'';

	size_t esc_len = mysql_real_escape_string(m, dest, str, str_len);
	dest += esc_len;

	*dest++ = '\'';
	*dest = '\0';

	*buflen += dest - orig_dest;
}
static inline void medmysql_buf_escape_c(MYSQL *m, size_t *buflen, const char *str, char *orig_dest,
		size_t sql_buffer_size)
{
	medmysql_buf_escape(m, buflen, str, strlen(str), orig_dest, sql_buffer_size);
}
static inline void medmysql_buf_escape_gstring(MYSQL *m, const char *str, GString *s)
{
    size_t str_len = strlen(str);
    size_t orig_size = s->len;
    size_t req_size = MED_SQL_BUF_LEN(orig_size, str_len);
    g_string_set_size(s, req_size);
    char *dst = s->str + orig_size;
    medmysql_buf_escape(m, &orig_size, str, str_len, dst, s->len);
    g_string_set_size(s, orig_size);
}

#define BUFPRINT(x...)    g_string_append_printf(sql_buffer, x)
#define BUFESCAPE(x) medmysql_buf_escape_gstring(med_handler->m, x, sql_buffer)

/**********************************************************************/
int medmysql_insert_records(GQueue *records, const char *table)
{
    int ret = 0, entries = 0;

    if (!records->length)
        return 0;

    GString *sql_buffer = g_string_sized_new(records->length * 512);
    BUFPRINT("INSERT INTO kamailio.acc_%s " \
        "(sip_code,sip_reason,method,callid,time,time_hires,src_leg,dst_leg,branch_id) VALUES ",
         table);
    
    for (GList *l = records->head; l; l = l->next) {
        med_entry_t *e = l->data;

        // this is only used for inserting redis entries into mysql
        if (!e->redis)
            continue;

        BUFPRINT("(");
        BUFESCAPE(e->sip_code);
        BUFPRINT(",");
        BUFESCAPE(e->sip_reason);
        BUFPRINT(",");
        BUFESCAPE(e->sip_method);
        BUFPRINT(",");
        BUFESCAPE(e->callid);
        BUFPRINT(",");
        BUFESCAPE(e->timestamp);
        BUFPRINT(",'%f',", e->unix_timestamp);
        BUFESCAPE(e->src_leg);
        BUFPRINT(",");
        BUFESCAPE(e->dst_leg);
        BUFPRINT(",");
        BUFESCAPE(e->branch_id);
        BUFPRINT("),");

        entries++;
    }
    if (!entries)
        goto out;

    g_string_set_size(sql_buffer, sql_buffer->len - 1);

    L_DEBUG("Issuing record insert query: %s\n", sql_buffer->str);
    
    ret = medmysql_query_wrapper(med_handler, sql_buffer->str, sql_buffer->len);
    if (ret != 0)
    {
        L_ERROR("Error executing query '%s': %s",
                sql_buffer->str, mysql_error(med_handler->m));
    }

out:
    g_string_free(sql_buffer, TRUE);
    return ret;
}

/**********************************************************************/
gboolean medmysql_fetch_callids(GQueue *output)
{
    MYSQL_RES *res;
    MYSQL_ROW row;
    /* char query[1024] = ""; */

    /* g_strlcpy(query, MED_CALLID_QUERY, sizeof(query)); */

    /*L_DEBUG("q='%s'", query);*/

    if(medmysql_query_wrapper(med_handler, MED_CALLID_QUERY, strlen(MED_CALLID_QUERY)) != 0)
    {
        L_CRITICAL("Error getting acc callids: %s",
                mysql_error(med_handler->m));
        return FALSE;
    }

    res = mysql_store_result(med_handler->m);

    while((row = mysql_fetch_row(res)) != NULL)
    {
        if(row[0] == NULL)
        {
            g_queue_push_tail(output, g_strdup("0"));
        } else {
            g_queue_push_tail(output, g_strdup(row[0]));
        }

        /*L_DEBUG("callid[%"PRIu64"]='%s'", i, c->value);*/

        if (check_shutdown()) {
            mysql_free_result(res);
            g_queue_clear_full(output, free);
            return FALSE;
        }
    }

    mysql_free_result(res);
    return TRUE;
}

/**********************************************************************/
int medmysql_fetch_records(char *callid,
        GQueue *entries, int warn_empty,
        records_filter_func filter, void *filter_data)
{
    MYSQL_RES *res;
    MYSQL_ROW row;
    size_t callid_len = strlen(callid);
    char query[strlen(MED_FETCH_QUERY) + callid_len * 7 + 1];
    int ret = 0;
    int len;
    unsigned long long count = 0;

    char esc_callid[callid_len*2+1];

    mysql_real_escape_string(med_handler->m, esc_callid, callid, callid_len);

    len = snprintf(query, sizeof(query), MED_FETCH_QUERY,
        esc_callid,
        esc_callid, esc_callid,
        esc_callid, esc_callid,
        esc_callid, esc_callid);

    assert(len > 0 && (size_t)len < sizeof(query)); /* truncated - internal bug */

    /*L_DEBUG("q='%s'", query);*/

    if(medmysql_query_wrapper(med_handler, query, len) != 0)
    {
        L_CRITICAL("Error getting acc records for callid '%s': %s",
                callid, mysql_error(med_handler->m));
        return -1;
    }

    res = mysql_store_result(med_handler->m);
    count = mysql_num_rows(res);
    if(count == 0)
    {
        if (warn_empty)
            L_CRITICAL("No records found for callid '%s'!",
                    callid);
        ret = -1;
        goto out;
    }

    ret = 0;
    while((row = mysql_fetch_row(res)) != NULL)
    {
        med_entry_t *e = g_slice_alloc0(sizeof(*e));

        g_strlcpy(e->sip_code, row[0], sizeof(e->sip_code));
        g_strlcpy(e->sip_reason, row[1], sizeof(e->sip_reason));
        g_strlcpy(e->sip_method, row[2], sizeof(e->sip_method));
        e->callid = g_strdup(row[3]);
        g_strlcpy(e->timestamp, row[4], sizeof(e->timestamp));
        e->unix_timestamp = atof(row[5]);
        e->src_leg = g_strdup(row[6] ? : "");
        e->dst_leg = g_strdup(row[7] ? : "");
        g_strlcpy(e->branch_id, row[8] ? : "", sizeof(e->branch_id));
        e->acc_ref = g_strdup(row[9]);
        e->valid = 1;

        cdr_parse_entry(e);

        if (!filter || filter(e, filter_data))
        {
            g_queue_push_tail(entries, e);

            if (records_handle_refer(entries, e, callid))
                ret = 1;
        }
        else
            med_entry_free(e);

        if (check_shutdown())
            return -1;
    }

out:
    mysql_free_result(res);
    return ret;
}


/**********************************************************************/
static int medmysql_batch_prepare(struct medmysql_str *str)
{
    const medmysql_batch_definition *def = str->def;
    struct medmysql_batches *batches = str->batches;

    unsigned int tail_room = def->min_string_tail_room;
    if (!tail_room)
        tail_room = 1024;

    if (str->len > (PACKET_SIZE - tail_room)) {
        if (def->single_flush_func) {
            if (def->single_flush_func(str))
                return -1;
        }
        if (def->full_flush_func) {
            if (def->full_flush_func(batches))
                return -1;
        }
    }

    if (str->len == 0)
        str->len = sprintf(str->str, "%s", def->sql_init_string);

    return 0;
}


/**********************************************************************/
int medmysql_trash_entries(const char *callid, struct medmysql_batches *batches)
{
    char esc_callid[strlen(callid)*2+1];

    mysql_real_escape_string(med_handler->m, esc_callid, callid, strlen(callid));

    if (medmysql_batch_prepare(&batches->acc_trash))
        return -1;
    batches->acc_trash.len += sprintf(batches->acc_trash.str + batches->acc_trash.len, "'%s',", esc_callid);

    return medmysql_delete_entries(esc_callid, batches);
}

/**********************************************************************/
int medmysql_backup_entries(const char *callid, struct medmysql_batches *batches)
{
    char esc_callid[strlen(callid)*2+1];

    mysql_real_escape_string(med_handler->m, esc_callid, callid, strlen(callid));

    if (medmysql_batch_prepare(&batches->acc_backup))
        return -1;
    batches->acc_backup.len += sprintf(batches->acc_backup.str + batches->acc_backup.len, "'%s',", esc_callid);

    return medmysql_delete_entries(esc_callid, batches);
}


/**********************************************************************/
static int medmysql_flush_all_med(struct medmysql_batches *batches) {
    if (medmysql_flush_medlist(&batches->acc_backup))
        return -1;
    if (medmysql_flush_medlist(&batches->acc_trash))
        return -1;
    if (medmysql_flush_medlist(&batches->to_delete))
        return -1;
    return 0;
}

int medmysql_delete_entries(const char *callid, struct medmysql_batches *batches)
{
    if (medmysql_batch_prepare(&batches->to_delete))
        return -1;
    batches->to_delete.len += sprintf(batches->to_delete.str + batches->to_delete.len, "'%s',", callid);

    return 0;
}

#define CDRPRINT(x)    batch->cdrs.len += sprintf(batch->cdrs.str + batch->cdrs.len, x)
#define CDRESCAPE(x) medmysql_buf_escape(med_handler->m, &batch->cdrs.len, x->str, x->len, batch->cdrs.str + batch->cdrs.len, PACKET_SIZE)
#define CDRESCAPE_C(x) medmysql_buf_escape_c(med_handler->m, &batch->cdrs.len, x, batch->cdrs.str + batch->cdrs.len, PACKET_SIZE)

/**********************************************************************/
static int medmysql_tag_record(GQueue *q, unsigned long cdr_id, unsigned long provider_id,
        unsigned long direction_id, const GString *value, double start_time, unsigned long tag_id)
{
    char esc_value[value->len*2+1];

    mysql_real_escape_string(med_handler->m, esc_value, value->str, value->len);

    cdr_tag_record *record = malloc(sizeof(*record));
    record->cdr_id = cdr_id;
    if (asprintf(&record->sql_record, "%lu, %lu, %lu, '%s', %f",
        provider_id, direction_id, tag_id, esc_value, start_time) <= 0)
    {
        free(record);
        return -1;
    }
    g_queue_push_tail(q, record);
    return 0;
}

static int medmysql_mos_record(GQueue *q, unsigned long cdr_id, double avg_score, int avg_packetloss,
        int avg_jitter, int avg_rtt, double start_time)
{
    cdr_tag_record *record = malloc(sizeof(*record));
    record->cdr_id = cdr_id;
    if (asprintf(&record->sql_record, "%.1f, %.1f, %.1f, %.1f, %.3f",
                avg_score, (double) avg_packetloss, (double) avg_jitter,
                (double) avg_rtt, start_time) <= 0)
    {
        free(record);
        return -1;
    }
    g_queue_push_tail(q, record);
    return 0;
}

static int medmysql_group_record(MYSQL *m, GQueue *q, unsigned long cdr_id, const GString *group, double start_time)
{
    char esc_group[group->len*2+1];

    mysql_real_escape_string(m, esc_group, group->str, group->len);

    cdr_tag_record *record = malloc(sizeof(*record));
    record->cdr_id = cdr_id;
    if (asprintf(&record->sql_record, "'%s', %.3f", esc_group, start_time) <= 0)
    {
        free(record);
        return -1;
    }
    g_queue_push_tail(q, record);
    return 0;
}


static int medmysql_tag_cdr(struct medmysql_cdr_batch *batch, unsigned long provider_id,
		unsigned long direction_id, const char *tag_name, const GString *tag_value,
		const cdr_entry_t *e)
{
    gpointer tag_id;

    if (!tag_value->len)
        return 0;

    if ((tag_id = g_hash_table_lookup(med_cdr_tag_table, tag_name)) == NULL) {
        L_WARNING("Call-Id '%s' has no cdr tag type '%s', '%s'",
                    e->call_id->str, tag_name, tag_value->str);
        return -1;
    }
    if (medmysql_tag_record(&batch->tags.q, batch->num_cdrs, provider_id,
            direction_id, tag_value, e->start_time,
            GPOINTER_TO_UINT(tag_id)))
        return -1;
    return 0;
}


int medmysql_insert_cdrs(cdr_entry_t *entries, uint64_t count, struct medmysql_batches *batches)
{
    uint64_t i;
    int gpp;
    int ret = 1; // default to intermediate

    for(i = 0; i < count; ++i)
    {
        cdr_entry_t *e = &(entries[i]);

        struct medmysql_cdr_batch *batch = &batches->cdr_batch;
        if (e->intermediate)
            batch = &batches->int_cdr_batch;
        else
            ret = 0; // if at least one record is not intermediate, we delete

        if (medmysql_batch_prepare(&batch->cdrs))
            return -1;

        char str_source_clir[4] = "";
        char str_split[4] = "";
        char str_init_time[32] = "";
        char str_start_time[32] = "";
        char str_duration[32] = "";
        char str_source_carrier_cost[32] = "";
        char str_source_reseller_cost[32] = "";
        char str_source_customer_cost[32] = "";
        char str_dest_carrier_cost[32] = "";
        char str_dest_reseller_cost[32] = "";
        char str_dest_customer_cost[32] = "";
        char str_source_accid[32] = "";
        char str_dest_accid[32] = "";
        snprintf(str_source_clir, sizeof(str_source_clir), "%u", e->source_clir);
        snprintf(str_split, sizeof(str_split), "%u", e->split);
        snprintf(str_init_time, sizeof(str_init_time), "%f", e->init_time);
        snprintf(str_start_time, sizeof(str_start_time), "%f", e->start_time);
        snprintf(str_duration, sizeof(str_duration), "%f", e->duration);
        snprintf(str_source_carrier_cost, sizeof(str_source_carrier_cost), "%u", e->source_carrier_cost);
        snprintf(str_source_reseller_cost, sizeof(str_source_reseller_cost), "%u", e->source_reseller_cost);
        snprintf(str_source_customer_cost, sizeof(str_source_customer_cost), "%u", e->source_customer_cost);
        snprintf(str_dest_carrier_cost, sizeof(str_dest_carrier_cost), "%u", e->destination_carrier_cost);
        snprintf(str_dest_reseller_cost, sizeof(str_dest_reseller_cost), "%u", e->destination_reseller_cost);
        snprintf(str_dest_customer_cost, sizeof(str_dest_customer_cost), "%u", e->destination_customer_cost);
        snprintf(str_source_accid, sizeof(str_source_accid), "%llu", (long long unsigned int) e->source_account_id);
        snprintf(str_dest_accid, sizeof(str_dest_accid), "%llu", (long long unsigned int) e->destination_account_id);

	const char *begin_ptr = batch->cdrs.str + batch->cdrs.len;

        CDRPRINT("(NULL, now(), ");
        CDRESCAPE(e->source_user_id);
        CDRPRINT(",");
        CDRESCAPE(e->source_provider_id);
        CDRPRINT(",");

        CDRESCAPE(e->source_ext_subscriber_id);
        CDRPRINT(",");
        CDRESCAPE(e->source_ext_contract_id);
        CDRPRINT(",");
        CDRESCAPE_C(str_source_accid);
        CDRPRINT(",");

        CDRESCAPE(e->source_user);
        CDRPRINT(",");
        CDRESCAPE(e->source_domain);
        CDRPRINT(",");
        CDRESCAPE(e->source_cli);
        CDRPRINT(",");
        CDRESCAPE_C(str_source_clir);
        CDRPRINT(",");
        CDRESCAPE(e->source_ip);
        CDRPRINT(",");
        CDRESCAPE(e->destination_user_id);
        CDRPRINT(",");
        CDRESCAPE(e->destination_provider_id);
        CDRPRINT(",");
        CDRESCAPE(e->destination_ext_subscriber_id);
        CDRPRINT(",");
        CDRESCAPE(e->destination_ext_contract_id);
        CDRPRINT(",");
        CDRESCAPE_C(str_dest_accid);
        CDRPRINT(",");
        CDRESCAPE(e->destination_user);
        CDRPRINT(",");
        CDRESCAPE(e->destination_domain);
        CDRPRINT(",");
        CDRESCAPE(e->destination_user_in);
        CDRPRINT(",");
        CDRESCAPE(e->destination_domain_in);
        CDRPRINT(",");
        CDRESCAPE(e->destination_dialed);
        CDRPRINT(",");
        CDRESCAPE(e->peer_auth_user);
        CDRPRINT(",");
        CDRESCAPE(e->peer_auth_realm);
        CDRPRINT(",");
        CDRESCAPE(e->call_type);
        CDRPRINT(",");
        CDRESCAPE(e->call_status);
        CDRPRINT(",");
        CDRESCAPE(e->call_code);
        CDRPRINT(",");
        CDRESCAPE_C(str_init_time);
        CDRPRINT(",");
        CDRESCAPE_C(str_start_time);
        CDRPRINT(",");
        CDRESCAPE_C(str_duration);
        CDRPRINT(",");
        CDRESCAPE(e->call_id);
        CDRPRINT(",");
        CDRESCAPE_C(str_source_carrier_cost);
        CDRPRINT(",");
        CDRESCAPE_C(str_source_reseller_cost);
        CDRPRINT(",");
        CDRESCAPE_C(str_source_customer_cost);
        CDRPRINT(",");
        CDRESCAPE_C(str_dest_carrier_cost);
        CDRPRINT(",");
        CDRESCAPE_C(str_dest_reseller_cost);
        CDRPRINT(",");
        CDRESCAPE_C(str_dest_customer_cost);
        CDRPRINT(",");
        CDRESCAPE_C(str_split);
        for(gpp = 0; gpp < 10; ++gpp)
        {
            if(e->source_gpp[gpp]->len > 0)
            {
                CDRPRINT(",");
                CDRESCAPE(e->source_gpp[gpp]);
            }
            else
            {
                CDRPRINT(",NULL");
            }
        }
        for(gpp = 0; gpp < 10; ++gpp)
        {
            if(e->destination_gpp[gpp]->len > 0)
            {
                CDRPRINT(",");
                CDRESCAPE(e->destination_gpp[gpp]);
            }
            else
            {
                CDRPRINT(",NULL");
            }
        }

        CDRPRINT(",");
        CDRESCAPE(e->source_lnp_prefix);
        CDRPRINT(",");
        CDRESCAPE(e->destination_lnp_prefix);
        CDRPRINT(",");
        CDRESCAPE(e->source_user_out);
        CDRPRINT(",");
        CDRESCAPE(e->destination_user_out);

        if(e->source_lnp_type->len > 0)
        {
            CDRPRINT(",");
            CDRESCAPE(e->source_lnp_type);
        }
        else
        {
            CDRPRINT(",NULL");
        }

        if(e->destination_lnp_type->len > 0)
        {
            CDRPRINT(",");
            CDRESCAPE(e->destination_lnp_type);
        }
        else
        {
            CDRPRINT(",NULL");
        }

        if (batch == &batches->int_cdr_batch) {
            CDRPRINT(",");
            CDRESCAPE(e->acc_ref);
        }

        CDRPRINT("),");

	const char *end_ptr = batch->cdrs.str + batch->cdrs.len;
	L_DEBUG("CDR entry to be written: %.*s", (int) (end_ptr - begin_ptr), begin_ptr);

        single_cdr *single = g_slice_alloc(sizeof(*single));
        single->begin = begin_ptr;
        single->end = end_ptr;
        single->cdr_id = batch->num_cdrs;
        g_queue_push_tail(&batch->cdrs.q, single);

        if (medmysql_tag_cdr(batch, medmysql_tag_provider_customer, medmysql_tag_direction_destination,
                    "furnished_charging_info", e->furnished_charging_info, e))
            return -1;
        if (medmysql_tag_cdr(batch, medmysql_tag_provider_customer, medmysql_tag_direction_source,
                    "header=P-Asserted-Identity", e->header_pai, e))
            return -1;
        if (medmysql_tag_cdr(batch, medmysql_tag_provider_customer, medmysql_tag_direction_source,
                    "header=Diversion", e->header_diversion, e))
            return -1;
        if (medmysql_tag_cdr(batch, medmysql_tag_provider_customer, medmysql_tag_direction_source,
                    "header=User-to-User", e->header_u2u, e))
            return -1;

        if (medmysql_tag_cdr(batch, medmysql_tag_provider_customer, medmysql_tag_direction_source,
                    "concurrent_calls_quota", e->source_concurrent_calls_quota, e))
            return -1;
        if (medmysql_tag_cdr(batch, medmysql_tag_provider_customer, medmysql_tag_direction_source,
                    "concurrent_calls_count", e->source_concurrent_calls_count, e))
            return -1;
        if (medmysql_tag_cdr(batch, medmysql_tag_provider_customer, medmysql_tag_direction_source,
                    "concurrent_calls_count_customer", e->source_concurrent_calls_count_customer, e))
            return -1;

        if (medmysql_tag_cdr(batch, medmysql_tag_provider_customer, medmysql_tag_direction_destination,
                    "concurrent_calls_quota", e->destination_concurrent_calls_quota, e))
            return -1;
        if (medmysql_tag_cdr(batch, medmysql_tag_provider_customer, medmysql_tag_direction_destination,
                    "concurrent_calls_count", e->destination_concurrent_calls_count, e))
            return -1;
        if (medmysql_tag_cdr(batch, medmysql_tag_provider_customer, medmysql_tag_direction_destination,
                    "concurrent_calls_count_customer", e->destination_concurrent_calls_count_customer, e))
            return -1;
        if (medmysql_tag_cdr(batch, medmysql_tag_provider_customer, medmysql_tag_direction_destination,
                    "hg_ext_response", e->hg_ext_response, e))
            return -1;
        if (medmysql_tag_cdr(batch, medmysql_tag_provider_customer, medmysql_tag_direction_source,
                    "header=History-Info", e->source_last_hih, e))
            return -1;

        if (e->mos.filled && batch->mos.def) {
            if (medmysql_mos_record(&batch->mos.q, batch->num_cdrs, e->mos.avg_score,
                        e->mos.avg_packetloss, e->mos.avg_jitter, e->mos.avg_rtt,
                        e->start_time))
                return -1;
        }

        if (medmysql_group_record(med_handler->m, &batch->group.q, batch->num_cdrs, e->group,
                    e->start_time))
            return -1;

        batch->num_cdrs++;

        // no check for return codes here we should keep on nevertheless
        medmysql_update_call_stat_info(e->call_code->str, e->start_time);

        if (check_shutdown())
            return -1;
    }

    /*L_DEBUG("q='%s'", query);*/

    return ret;
}

/**********************************************************************/
int medmysql_delete_intermediate(cdr_entry_t *entries, uint64_t count, struct medmysql_batches *batches)
{
    uint64_t i;

    for(i = 0; i < count; ++i)
    {
        cdr_entry_t *e = &(entries[i]);
        char esc_callid[e->call_id->len*2+1];

        mysql_real_escape_string(int_cdr_handler->m, esc_callid, e->call_id->str, e->call_id->len);

        if (medmysql_batch_prepare(&batches->int_cdr_delete))
            return -1;
        batches->int_cdr_delete.len += sprintf(batches->int_cdr_delete.str + batches->int_cdr_delete.len,
                "'%s',", esc_callid);
    }
    return 0;
}

/**********************************************************************/
int medmysql_update_call_stat_info(const char *call_code, const double start_time)
{
    if (!med_call_stat_info_table)
        return -1;

    char period[STAT_PERIOD_SIZE];
    time_t etime = (time_t)start_time;
    struct tm etm;
    char period_key[STAT_PERIOD_SIZE+4];
    struct medmysql_call_stat_info_t * period_t;

    if (!localtime_r(&etime, &etm)) {
        L_CRITICAL("Cannot get localtime: %s", strerror(errno));
        return -1;
    }

    switch (config_stats_period)
    {
        case MED_STATS_HOUR:
            strftime(period, STAT_PERIOD_SIZE, "%Y-%m-%d %H:00:00", &etm);
            break;
        case MED_STATS_DAY:
            strftime(period, STAT_PERIOD_SIZE, "%Y-%m-%d 00:00:00", &etm);
            break;
        case MED_STATS_MONTH:
            strftime(period, STAT_PERIOD_SIZE, "%Y-%m-01 00:00:00", &etm);
            break;
        default:
            L_CRITICAL("Undefinied or wrong config_stats_period %d",
                    config_stats_period);
            return -1;
    }

    snprintf(period_key, sizeof(period_key), "%s-%s", period, call_code);

    if ((period_t = g_hash_table_lookup(med_call_stat_info_table, &period_key)) == NULL) {
        period_t = malloc(sizeof(struct medmysql_call_stat_info_t));
        strcpy(period_t->period, period);
        g_strlcpy(period_t->call_code, call_code, sizeof(period_t->call_code));
        period_t->amount = 1;
        g_hash_table_insert(med_call_stat_info_table, strdup(period_key), period_t);
    } else {
        period_t->amount += 1;
    }

    return 0;
}

/**********************************************************************/
int medmysql_load_maps(GHashTable *ip_table, GHashTable *host_table, GHashTable *id_table)
{
    MYSQL_RES *res;
    MYSQL_ROW row;
    int ret = 0;
    /* char query[1024] = ""; */

    /* snprintf(query, sizeof(query), MED_LOAD_PEER_QUERY); */

    /* L_DEBUG("q='%s'", query); */
    if(medmysql_query_wrapper(prov_handler, MED_LOAD_PEER_QUERY, strlen(MED_LOAD_PEER_QUERY)) != 0)
    {
        L_CRITICAL("Error loading peer hosts: %s",
                mysql_error(prov_handler->m));
        return -1;
    }

    res = mysql_store_result(prov_handler->m);

    while((row = mysql_fetch_row(res)) != NULL)
    {
        if(row[0] == NULL || row[2] == NULL)
        {
            L_CRITICAL("Error loading peer hosts, a mandatory column is NULL");
            ret = -1;
            goto out;
        }

        if(ip_table != NULL)
        {
            if(g_hash_table_lookup(ip_table, row[0]) != NULL)
                L_DEBUG("Skipping duplicate IP '%s'", row[0]);
            else
                g_hash_table_insert(ip_table, strdup(row[0]), strdup(row[2]));
        }
        if(host_table != NULL && row[1] != NULL) // host column is optional
        {
            if(g_hash_table_lookup(host_table, row[1]) != NULL)
                L_DEBUG("Skipping duplicate host '%s'", row[1]);
            else
                g_hash_table_insert(host_table, strdup(row[1]), strdup(row[2]));
        }
        if (id_table)
            g_hash_table_insert(id_table, strdup(row[3]), strdup(row[2]));
    }

out:
    mysql_free_result(res);
    return ret;
}

/**********************************************************************/
static char *medmysql_lookup_cache_str(GHashTable *table, const char *key)
{
    med_cache_entry_t *e = g_hash_table_lookup(table, key);
    if (!e)
        return NULL;
    if (time(NULL) - e->created > MED_CACHE_DURATION) {
        g_hash_table_remove(table, key);
        return NULL;
    }
    L_DEBUG("Returning cached map entry: '%s' -> '%s'", key, e->str_value);
    return e->str_value;
}


void medmysql_cache_cleanup(GHashTable *table)
{
    GHashTableIter iter;
    g_hash_table_iter_init(&iter, table);
    gpointer value;
    time_t expiry = time(NULL) - MED_CACHE_DURATION;
    while (g_hash_table_iter_next(&iter, NULL, &value)) {
        med_cache_entry_t *e = value;
        if (e->created > expiry)
            continue;
        g_hash_table_iter_remove(&iter);
    }
}


static void medmysql_insert_cache_str(GHashTable *table, const char *key, char *val)
{
    med_cache_entry_t *e = g_slice_alloc0(sizeof(*e));
    e->created = time(NULL);
    e->str_value = val; // allocated per g_strdup
    g_hash_table_replace(table, g_strdup(key), e);
}


static char *medmysql_select_one(medmysql_handler *mysql, const char *query, size_t query_len)
{
    int err = medmysql_query_wrapper(mysql, query, query_len);
    if (err != 0) {
        L_ERROR("Error executing query '%.*s': %s",
                (int) query_len, query, mysql_error(mysql->m));
        return NULL;
    }

    char *ret = NULL;

    MYSQL_RES *res = mysql_store_result(mysql->m);
    unsigned long long rows = mysql_num_rows(res);
    if (rows > 0) {
        if (rows > 1)
            L_WARNING("More than one row returned from query '%.*s' (%llu rows)",
                    (int) query_len, query, rows);

        MYSQL_ROW row = mysql_fetch_row(res);
        if (!row) {
            L_ERROR("Failed to fetch row from query '%.*s': %s",
                    (int) query_len, query, mysql_error(mysql->m));
        }
        else {
            const char *val = row[0];
            if (!val)
                L_ERROR("NULL value returned from query '%.*s'",
                        (int) query_len, query);
            else
                ret = g_strdup(val);
        }
    }

    mysql_free_result(res);

    return ret;
}


char *medmysql_lookup_uuid(const char *uuid)
{
    char *ret = medmysql_lookup_cache_str(med_uuid_cache, uuid);
    if (ret)
        return ret;

    // query DB

    static const char *query_prefix = "SELECT r.contract_id FROM billing.voip_subscribers vs, " \
        "billing.contracts c, billing.contacts ct, billing.resellers r WHERE c.id = vs.contract_id AND " \
        "c.contact_id = ct.id AND ct.reseller_id = r.id AND vs.uuid = ";
    size_t query_len = strlen(query_prefix); // compile-time constant

    char query[MED_SQL_BUF_LEN(query_len, strlen(uuid))];
    strcpy(query, query_prefix);
    char *buf_ptr = query + query_len;
    medmysql_buf_escape_c(prov_handler->m, &query_len, uuid, buf_ptr, sizeof(query));

    L_DEBUG("UUID lookup with query: '%.*s'\n", (int) query_len, query);

    ret = medmysql_select_one(prov_handler, query, query_len);
    if (!ret)
        return NULL;

    L_DEBUG("UUID query returned: '%.*s' -> '%s'\n", (int) query_len, query, ret);

    medmysql_insert_cache_str(med_uuid_cache, uuid, ret);

    return ret;
}


int medmysql_load_db_ids() {
    if (!(medmysql_cdr_auto_increment_value
            = medmysql_get_num_col(cdr_handler, "select @@auto_increment_increment")))
        return -1;
    if (!(medmysql_tag_provider_customer
            = medmysql_get_num_col(cdr_handler,
                "select id from cdr_provider where type = 'customer'")))
        return -1;
    if (!(medmysql_tag_provider_carrier
            = medmysql_get_num_col(cdr_handler,
                "select id from cdr_provider where type = 'carrier'")))
        return -1;
    if (!(medmysql_tag_provider_reseller
            = medmysql_get_num_col(cdr_handler,
                "select id from cdr_provider where type = 'reseller'")))
        return -1;
    if (!(medmysql_tag_direction_source
            = medmysql_get_num_col(cdr_handler,
                "select id from cdr_direction where type = 'source'")))
        return -1;
    if (!(medmysql_tag_direction_destination
            = medmysql_get_num_col(cdr_handler,
                "select id from cdr_direction where type = 'destination'")))
        return -1;

    return 0;
}

int medmysql_load_cdr_tag_ids(GHashTable *cdr_tag_table)
{
    MYSQL_RES *res;
    MYSQL_ROW row;
    int ret = 0;
    /* char query[1024] = ""; */
    gpointer key;
    gpointer tag_id;

    /* snprintf(query, sizeof(query), MED_LOAD_CDR_TAG_IDS_QUERY); */

    /* L_DEBUG("q='%s'", query); */
    if(medmysql_query_wrapper(cdr_handler, MED_LOAD_CDR_TAG_IDS_QUERY, strlen(MED_LOAD_CDR_TAG_IDS_QUERY)) != 0)
    {
        L_CRITICAL("Error loading cdr tag ids: %s",
                mysql_error(prov_handler->m));
        return -1;
    }

    res = mysql_store_result(cdr_handler->m);

    while((row = mysql_fetch_row(res)) != NULL)
    {
        if(row[0] == NULL || row[1] == NULL)
        {
            L_CRITICAL("Error loading cdr tag ids, a column is NULL");
            ret = -1;
            goto out;
        }

        tag_id = GUINT_TO_POINTER(strtoul(row[0], NULL, 10));
        if(tag_id == NULL)
        {
            L_CRITICAL("Error allocating cdr tag id memory: %s", strerror(errno));
            ret = -1;
            goto out;
        }

        key = (gpointer)g_strdup(row[1]);
        g_hash_table_insert(cdr_tag_table, key, tag_id);
    }

out:
    mysql_free_result(res);
    return ret;
}


static int medmysql_handler_transaction(medmysql_handler *h) {
    if (h->is_transaction)
        return 0;
    if (medmysql_query_wrapper(h, "start transaction", 17))
        return -1;
    h->is_transaction = 1;
    return 0;
}

static int medmysql_handler_commit(medmysql_handler *h) {
    if (!h->is_transaction)
        return 0;
    if (medmysql_query_wrapper(h, "commit", 6))
        return -1;
    h->is_transaction = 0;
    __g_queue_clear_full(&h->transaction_statements, statement_free);
    return 0;
}

static void medmysql_str_init(struct medmysql_str *str, const medmysql_batch_definition *def,
        struct medmysql_batches *batches,
        struct medmysql_cdr_batch *batch)
{
    str->len = 0;
    str->def = def;
    str->batches = batches;
    str->cdr_batch = batch;
    g_queue_init(&str->q);
}

int medmysql_batch_start(struct medmysql_batches *batches) {
    if (medmysql_handler_transaction(cdr_handler))
        return -1;
    if (medmysql_handler_transaction(int_cdr_handler))
        return -1;
    if (medmysql_handler_transaction(med_handler))
        return -1;

    if (!med_call_stat_info_table)
        med_call_stat_info_table = g_hash_table_new_full(g_str_hash, g_str_equal, free, free);

    medmysql_str_init(&batches->cdr_batch.cdrs, &medmysql_cdr_def, batches, &batches->cdr_batch);
    medmysql_str_init(&batches->cdr_batch.tags, &medmysql_tag_def, batches, &batches->cdr_batch);
    medmysql_str_init(&batches->cdr_batch.mos, &medmysql_mos_def, batches, &batches->cdr_batch);
    medmysql_str_init(&batches->cdr_batch.group, &medmysql_group_def, batches, &batches->cdr_batch);

    batches->cdr_batch.num_cdrs = 0;

    medmysql_str_init(&batches->int_cdr_batch.cdrs, &medmysql_int_cdr_def, batches, &batches->int_cdr_batch);
    medmysql_str_init(&batches->int_cdr_batch.tags, &medmysql_int_tag_def, batches, &batches->int_cdr_batch);
    medmysql_str_init(&batches->int_cdr_batch.mos, NULL, batches, &batches->int_cdr_batch);
    medmysql_str_init(&batches->int_cdr_batch.group, &medmysql_int_group_def, batches, &batches->int_cdr_batch);

    batches->int_cdr_batch.num_cdrs = 0;

    medmysql_str_init(&batches->acc_backup, &medmysql_backup_def, batches, NULL);
    medmysql_str_init(&batches->acc_trash, &medmysql_trash_def, batches, NULL);
    medmysql_str_init(&batches->to_delete, &medmysql_delete_def, batches, NULL);
    medmysql_str_init(&batches->int_cdr_delete, &medmysql_del_int_cdr_def, batches, NULL);

    return 0;
}


static int medmysql_flush_med_str(struct medmysql_str *str) {
    const medmysql_batch_definition *def = str->def;
    if (!def)
        return 0;

    if (str->len == 0)
        return 0;
    if (str->str[str->len - 1] != ',')
        return 0;

    str->len--;
    str->str[str->len] = '\0';

    if (def->sql_finish_string)
        str->len += sprintf(str->str + str->len, "%s", def->sql_finish_string);

    L_DEBUG("SQL flush med str\n");
    L_DEBUG("SQL: %.*s\n", (int) str->len, str->str);

    if (medmysql_query_wrapper_tx(*def->handler_ptr, str->str, str->len) != 0) {
        str->len = 0;
        L_CRITICAL("Error executing query: %s",
                mysql_error((*def->handler_ptr)->m));
        if (str->len <= 500)
            L_CRITICAL("Failed query: '%s'", str->str);
        else
            L_CRITICAL("Failed query: '%.*s'...", 500, str->str);
        return -1;
    }

    str->len = 0;
    return 0;
}


static int medmysql_write_tag_records(struct medmysql_str *str, unsigned long long auto_id)
{
    if (!str->def)
        return 0;
    cdr_tag_record *record;
    GQueue *q = &str->q;
    while ((record = g_queue_pop_head(q))) {
        if (medmysql_batch_prepare(str))
            return -1;
        record->cdr_id = auto_id + record->cdr_id * medmysql_cdr_auto_increment_value;
        str->len += sprintf(str->str + str->len,
                "(%llu, %s),", record->cdr_id, record->sql_record);
        free(record->sql_record);
        free(record);
    }
    return 0;
}
static int medmysql_write_cdr_tags(struct medmysql_cdr_batch *batch, unsigned long long auto_id) {
    if (medmysql_write_tag_records(&batch->tags, auto_id))
        return -1;
    if (medmysql_write_tag_records(&batch->mos, auto_id))
        return -1;
    if (medmysql_write_tag_records(&batch->group, auto_id))
        return -1;
    return 0;
}


static int medmysql_write_single_tags(struct medmysql_str *str, unsigned long cdr_id) {
    const medmysql_batch_definition *def = str->def;
    if (!def)
        return 0;

    int ret = 0;

    // iterate list, skip/free old entries, process current entries, stop at later entry
    cdr_tag_record *record;
    while ((record = g_queue_pop_head(&str->q))) {
        if (record->cdr_id < cdr_id)
            goto next; // old entry, skip and free
        if (record->cdr_id > cdr_id) {
            // gone past - end of list for now, push back entry for later
            g_queue_push_head(&str->q, record);
            return 0;
        }

        // process this entry: construct SQL statement
        char *stmt;
        if (def->sql_finish_string)
            stmt = g_strdup_printf("%s(%lu, %s)%s",
                    def->sql_init_string,
                    cdr_id,
                    record->sql_record,
                    def->sql_finish_string);
        else
            stmt = g_strdup_printf("%s(%lu, %s)",
                    def->sql_init_string,
                    cdr_id,
                    record->sql_record);

        L_DEBUG("SQL single tag/group statement\n");
        L_DEBUG("SQL: %s\n", stmt);

        if (medmysql_query_wrapper_tx(*def->handler_ptr, stmt, strlen(stmt))) {
            L_CRITICAL("Error executing query: %s",
                    mysql_error((*def->handler_ptr)->m));
            L_CRITICAL("Failed query: '%s'", stmt);
            ret = -1;
        }

        g_free(stmt);

next:
        free(record->sql_record);
        free(record);
    }
    return ret;
}


// retry the CDR inserts one by one
static int medmysql_flush_single_cdrs(struct medmysql_cdr_batch *batch) {
    const medmysql_batch_definition *def = batch->cdrs.def;
    if (!def)
        return -1;

    unsigned long failed = 0;

    single_cdr *single;
    while ((single = g_queue_pop_head(&batch->cdrs.q))) {
        // construct single SQL insert statement
        char *stmt;
        if (def->sql_finish_string)
            stmt = g_strdup_printf("%s%.*s%s",
                    def->sql_init_string,
                    (int) (single->end - single->begin) - 1,
                    single->begin,
                    def->sql_finish_string);
        else
            stmt = g_strdup_printf("%s%.*s",
                    def->sql_init_string,
                    (int) (single->end - single->begin) - 1,
                    single->begin);

        unsigned long cdr_id = single->cdr_id;

        g_slice_free1(sizeof(*single), single);

        L_DEBUG("SQL single statement\n");
        L_DEBUG("SQL: %s\n", stmt);

        if (medmysql_query_wrapper_tx(*def->handler_ptr, stmt, strlen(stmt)) == 0) {
            // insert OK
            unsigned long auto_id = mysql_insert_id((*def->handler_ptr)->m);
            if (!auto_id) {
                L_CRITICAL("Received zero auto-ID from SQL");
                g_free(stmt);
                return -1;
            }

            if (medmysql_write_single_tags(&batch->tags, cdr_id))
                failed++;
            if (medmysql_write_single_tags(&batch->mos, cdr_id))
                failed++;
            if (medmysql_write_single_tags(&batch->group, cdr_id))
                failed++;
        }
        else {
            L_CRITICAL("Error executing query: %s",
                    mysql_error((*def->handler_ptr)->m));
            L_CRITICAL("Failed query: '%s'", stmt);
            failed++;
        }

        g_free(stmt);
    }

    if (failed) {
        double failed_pct = (double) failed * 100. / (double) batch->num_cdrs;
        if (failed_pct > 10.) {
            L_CRITICAL("Large number of SQL failures during insert (%lu out of %lu, %.1f%%), bailing",
                    failed, batch->num_cdrs, failed_pct);
            return -1;
        }
        L_WARNING("Ignoring %lu failed inserts (out of %lu = %.1f%%), continuing",
                failed, batch->num_cdrs, failed_pct);
    }

    return 0;
}


static int medmysql_flush_cdr_batch(struct medmysql_cdr_batch *batch) {

    // TODO: the config option is a bit misleading
    if (config_dumpcdr && batch->cdrs.len) {
        FILE *qlog;

        qlog = fopen("/var/log/ngcp/cdr-query.log", "a");
        if(qlog == NULL)
        {
            L_CRITICAL("Failed to open cdr query log file '/var/log/ngcp/cdr-query.log': %s", strerror(errno));
            return -1;
        }
        if(fputs(batch->cdrs.str, qlog) == EOF)
        {
            L_CRITICAL("Failed to write to cdr query log file '/var/log/ngcp/cdr-query.log': %s", strerror(errno));
        }
        fclose(qlog);
    }

    if (!batch->cdrs.len)
        return 0;


    if (medmysql_flush_med_str(&batch->cdrs)) {
        if (medmysql_flush_single_cdrs(batch))
            return -1;
    }
    else {
        unsigned long auto_id = mysql_insert_id((*batch->cdrs.def->handler_ptr)->m);
        if (!auto_id) {
            L_CRITICAL("Received zero auto-ID from SQL");
            return -1;
        }

        single_cdr *single;
        while ((single = g_queue_pop_head(&batch->cdrs.q)))
            g_slice_free1(sizeof(*single), single);

        if (medmysql_write_cdr_tags(batch, auto_id))
            return -1;
        if (medmysql_flush_med_str(&batch->tags))
            return -1;
        if (medmysql_flush_med_str(&batch->mos))
            return -1;
        if (medmysql_flush_med_str(&batch->group))
            return -1;
    }

    batch->num_cdrs = 0;

    return 0;
}

static int medmysql_flush_cdr(struct medmysql_batches *batches) {
    if (medmysql_flush_med_str(&batches->int_cdr_delete))
        return -1;
    return medmysql_flush_cdr_batch(&batches->cdr_batch);
}

static int medmysql_flush_int_cdr(struct medmysql_batches *batches) {
    return medmysql_flush_cdr_batch(&batches->int_cdr_batch);
}

static int medmysql_flush_medlist(struct medmysql_str *str) {
    if (medmysql_flush_med_str(str)) {
        critical("Failed to execute potentially crucial SQL query, check LOG for details");
        return -1;
    }

    return 0;
}

static int medmysql_flush_call_stat_info() {
    if (!stats_handler)
        return 0;
    if (!med_call_stat_info_table)
        return 0;

    GHashTable * call_stat_info = med_call_stat_info_table;
    struct medmysql_str query;
    struct medmysql_call_stat_info_t * period_t;

    GList * keys = g_hash_table_get_keys(call_stat_info);
    GList * iter;
    for (iter = keys; iter != NULL; iter = iter->next) {
        char * period_key = iter->data;
         if ((period_t = g_hash_table_lookup(call_stat_info, period_key)) == NULL) {
            _LOG(LOG_CRIT,
                    "Error dumping call stats info: no data for period_key %s\n",
                    period_key
                  );
            return -1;
        }

        query.len = sprintf(query.str,
                "insert into %s.call_info set period='%s', sip_code='%s', amount=%" PRIu64 " on duplicate key update period='%s', sip_code='%s', amount=(amount+%" PRIu64 ");",
                config_stats_db,
                period_t->period, period_t->call_code, period_t->amount,
                period_t->period, period_t->call_code, period_t->amount
            );

        //L_DEBUG("updating call stats info: %s -- %s", period_t->call_code, period_t->period);
        //L_DEBUG("sql: %s", query.str);

        if(medmysql_query_wrapper(stats_handler, query.str, query.len) != 0)
        {
            L_CRITICAL("Error executing call info stats query: %s",
                    mysql_error(stats_handler->m));
            L_CRITICAL("stats query: %s", query.str);
            critical("Failed to execute potentially crucial SQL query, check LOG for details");
            return -1;
        }
        period_t->amount = 0; // reset code counter on success
    }
    g_list_free(keys);
    g_list_free(iter);

    return 0;
}

int medmysql_batch_end(struct medmysql_batches *batches) {
    if (medmysql_flush_cdr(batches) || check_shutdown())
        return -1;
    if (medmysql_flush_int_cdr(batches) || check_shutdown())
        return -1;
    if (medmysql_flush_all_med(batches) || check_shutdown())
        return -1;
    if (medmysql_flush_call_stat_info() || check_shutdown())
        return -1;

    if (medmysql_handler_commit(cdr_handler))
        return -1;
    if (medmysql_handler_commit(int_cdr_handler))
        return -1;
    if (medmysql_handler_commit(med_handler))
        return -1;

    return 0;
}
