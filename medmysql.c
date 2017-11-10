#include <my_global.h>
#include <m_string.h>
#include <mysql.h>
#include <mysql/errmsg.h>
#include <mysql/mysqld_error.h>
#include <assert.h>

#include "medmysql.h"
#include "config.h"

#define _TEST_SIMULATE_SQL_ERRORS 0

#define PBXSUFFIX "_pbx-1"
#define XFERSUFFIX "_xfer-1"

#define MED_CALLID_QUERY "select a.callid from acc a" \
	" where a.method = 'INVITE' " \
	  " and (a.sip_code != '200' " \
			" OR EXISTS " \
				" (select b.id from acc b " \
				  " where b.callid = a.callid " \
					" and b.method = 'BYE' " \
				   " limit 1) " \
			" OR EXISTS " \
				" (select b.id from acc b " \
				  " where b.callid = concat(a.callid, '"PBXSUFFIX"') " \
					" and b.method = 'BYE' " \
				  " limit 1) " \
			" OR EXISTS " \
				" (select b.id from acc b " \
				  " where b.callid = concat(a.callid, '"XFERSUFFIX"') " \
					" and b.method = 'BYE' " \
				  " limit 1) " \
		  " ) " \
   " group by a.callid limit 0,200000"

#define MED_FETCH_QUERY "(select distinct sip_code, sip_reason, method, callid, time, time_hires, " \
	"src_leg, dst_leg " \
	"from acc where method = 'INVITE' and callid = '%s' order by time_hires asc) " \
	"union all " \
	"(select distinct sip_code, sip_reason, method, callid, time, time_hires, " \
	"src_leg, dst_leg " \
	"from acc where method = 'BYE' and callid in ('%s', '%s"PBXSUFFIX"') " \
	"order by length(callid) asc, time_hires asc) " \
	"union all " \
	"(select distinct sip_code, sip_reason, method, callid, time, time_hires, " \
	"src_leg, dst_leg " \
	"from acc where method = 'BYE' and callid in ('%s', '%s"XFERSUFFIX"') " \
	"order by length(callid) asc, time_hires asc)"

#define MED_LOAD_PEER_QUERY "select h.ip, h.host, g.peering_contract_id, h.id " \
	"from provisioning.voip_peer_hosts h, provisioning.voip_peer_groups g " \
	"where g.id = h.group_id"
#define MED_LOAD_UUID_QUERY "select vs.uuid, r.contract_id from billing.voip_subscribers vs, " \
	"billing.contracts c, billing.contacts ct, billing.resellers r where c.id = vs.contract_id and " \
	"c.contact_id = ct.id and ct.reseller_id = r.id"

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

static medmysql_handler *cdr_handler;
static medmysql_handler *med_handler;
static medmysql_handler *prov_handler;
static medmysql_handler *stats_handler;

static int medmysql_flush_cdr(struct medmysql_batches *);
static int medmysql_flush_medlist(struct medmysql_str *);
static int medmysql_flush_call_stat_info();
static void medmysql_handler_close(medmysql_handler **h);
static int medmysql_handler_transaction(medmysql_handler *h);


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
		syslog(LOG_INFO, "Simulating SQL error - statement '%.*s'",
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
			syslog(LOG_WARNING, "Lost connection to SQL server during query, retrying...");
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
		syslog(LOG_CRIT, "SQL mode is not in transaction");
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
			syslog(LOG_WARNING, "Got error %u from SQL server during transaction, retrying...",
					err);
			err = medmysql_real_query_errno(mysql->m, "rollback", 8);
			if (err) {
				syslog(LOG_CRIT, "Got error %u from SQL during rollback",
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
			syslog(LOG_CRIT, "Out of memory (malloc statement_str)");
			return -1;
		}
		stm->str = malloc(length);
		if (!stm->str) {
			syslog(LOG_CRIT, "Out of memory (malloc statement_str body)");
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
	my_bool recon = 1;

	ret = malloc(sizeof(*ret));
	if (!ret) {
		syslog(LOG_CRIT, "Out of memory (malloc in medmysql_handler_init)");
		return NULL;
	}
	memset(ret, 0, sizeof(*ret));
	ret->name = name;
	g_queue_init(&ret->transaction_statements);
	ret->m = mysql_init(NULL);
	if (!ret->m) {
		syslog(LOG_CRIT, "Out of memory (mysql_init)");
		goto err;
	}

	if(!mysql_real_connect(ret->m,
				host, user, pass,
				db, port, NULL, 0))
	{
		syslog(LOG_CRIT, "Error connecting to %s db: %s", name, mysql_error(ret->m));
		goto err;
	}
	if(mysql_options(ret->m, MYSQL_OPT_RECONNECT, &recon) != 0)
	{
		syslog(LOG_CRIT, "Error setting reconnect-option for %s db: %s", name, mysql_error(ret->m));
		goto err;
	}
	if(mysql_autocommit(ret->m, 1) != 0)
	{
		syslog(LOG_CRIT, "Error setting autocommit=1 for %s db: %s", name,
				mysql_error(ret->m));
		goto err;
	}

	return ret;

err:
	medmysql_handler_close(&ret);
	return NULL;
}

/**********************************************************************/
int medmysql_init()
{
	cdr_handler = medmysql_handler_init("CDR",
				config_cdr_host, config_cdr_user, config_cdr_pass,
				config_cdr_db, config_cdr_port);
	if (!cdr_handler)
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
		syslog(LOG_WARNING, "Closing DB handle with still %u statements in queue",
				(*h)->transaction_statements.length);
	__g_queue_clear_full(&(*h)->transaction_statements, statement_free);

	free(*h);
	*h = NULL;
}

void medmysql_cleanup()
{
	medmysql_handler_close(&cdr_handler);
	medmysql_handler_close(&med_handler);
	medmysql_handler_close(&prov_handler);
	medmysql_handler_close(&stats_handler);
}

/**********************************************************************/
med_callid_t *medmysql_fetch_callids(uint64_t *count)
{
	MYSQL_RES *res;
	MYSQL_ROW row;
	/* char query[1024] = ""; */
	size_t callid_size;
	uint64_t i = 0;
	med_callid_t *callids = NULL;

	*count = (uint64_t) -1; /* non-zero count and return of NULL == error */

	/* g_strlcpy(query, MED_CALLID_QUERY, sizeof(query)); */

	/*syslog(LOG_DEBUG, "q='%s'", query);*/

	if(medmysql_query_wrapper(med_handler, MED_CALLID_QUERY, strlen(MED_CALLID_QUERY)) != 0)
	{
		syslog(LOG_CRIT, "Error getting acc callids: %s",
				mysql_error(med_handler->m));
		return NULL;
	}

	res = mysql_store_result(med_handler->m);
	*count = mysql_num_rows(res);
	if(*count == 0)
	{
		goto out;
	}

	callid_size = sizeof(med_callid_t) * (*count);
	callids = malloc(callid_size);
	if(callids == NULL)
	{
		syslog(LOG_CRIT, "Error allocating callid memory: %s", strerror(errno));
		free(callids);
		callids = NULL;
		goto out;
	}

	memset(callids, '\0', callid_size);

	while((row = mysql_fetch_row(res)) != NULL)
	{
		med_callid_t *c = &callids[i++];
		if(row == NULL || row[0] == NULL)
		{
			g_strlcpy(c->value, "0", sizeof(c->value));
		} else {
			g_strlcpy(c->value, row[0], sizeof(c->value));
		}

		/*syslog(LOG_DEBUG, "callid[%"PRIu64"]='%s'", i, c->value);*/

		if (check_shutdown()) {
			free(callids);
			return NULL;
		}
	}

out:
	mysql_free_result(res);
	return callids;
}

/**********************************************************************/
int medmysql_fetch_records(med_callid_t *callid,
		med_entry_t **entries, uint64_t *count)
{
	MYSQL_RES *res;
	MYSQL_ROW row;
	char query[strlen(MED_FETCH_QUERY) + sizeof(callid->value) * 5 + 1];
	size_t entry_size;
	uint64_t i = 0;
	int ret = 0;
	int len;

	*count = 0;

	len = snprintf(query, sizeof(query), MED_FETCH_QUERY,
		callid->value,
		callid->value, callid->value,
		callid->value, callid->value);

	assert(len < sizeof(query)); /* truncated - internal bug */

	/*syslog(LOG_DEBUG, "q='%s'", query);*/

	if(medmysql_query_wrapper(med_handler, query, len) != 0)
	{
		syslog(LOG_CRIT, "Error getting acc records for callid '%s': %s",
				callid->value, mysql_error(med_handler->m));
		return -1;
	}

	res = mysql_store_result(med_handler->m);
	*count = mysql_num_rows(res);
	if(*count == 0)
	{
		syslog(LOG_CRIT, "No records found for callid '%s'!",
				callid->value);
		ret = -1;
		goto out;
	}

	entry_size = (*count) * sizeof(med_entry_t);
	*entries = (med_entry_t*)malloc(entry_size);
	if(*entries == NULL)
	{
		syslog(LOG_CRIT, "Error allocating memory for record entries: %s",
				strerror(errno));
		ret = -1;
		goto out;

	}
	memset(*entries, 0, entry_size);

	while((row = mysql_fetch_row(res)) != NULL)
	{
		med_entry_t *e = &(*entries)[i++];

		g_strlcpy(e->sip_code, row[0], sizeof(e->sip_code));
		g_strlcpy(e->sip_reason, row[1], sizeof(e->sip_reason));
		g_strlcpy(e->sip_method, row[2], sizeof(e->sip_method));
		g_strlcpy(e->callid, row[3], sizeof(e->callid));
		g_strlcpy(e->timestamp, row[4], sizeof(e->timestamp));
		e->unix_timestamp = atof(row[5]);
		g_strlcpy(e->src_leg, row[6], sizeof(e->src_leg));
		g_strlcpy(e->dst_leg, row[7], sizeof(e->dst_leg));
		e->valid = 1;

		if (check_shutdown())
			return -1;
	}

out:
	mysql_free_result(res);
	return ret;
}

/**********************************************************************/
int medmysql_trash_entries(const char *callid, struct medmysql_batches *batches)
{
	if (batches->acc_trash.len > (PACKET_SIZE - 1024)) {
		if (medmysql_flush_medlist(&batches->acc_trash))
			return -1;
	}

	if (batches->acc_trash.len == 0)
		batches->acc_trash.len = sprintf(batches->acc_trash.str, "insert into acc_trash (method, from_tag, to_tag, callid, sip_code, sip_reason, time, time_hires, src_leg, dst_leg, dst_user, dst_ouser, dst_domain, src_user, src_domain) select method, from_tag, to_tag, callid, sip_code, sip_reason, time, time_hires, src_leg, dst_leg, dst_user, dst_ouser, dst_domain, src_user, src_domain from acc where callid in (");

	batches->acc_trash.len += sprintf(batches->acc_trash.str + batches->acc_trash.len, "'%s',", callid);

	return medmysql_delete_entries(callid, batches);
}

/**********************************************************************/
int medmysql_backup_entries(const char *callid, struct medmysql_batches *batches)
{
	if (batches->acc_backup.len > (PACKET_SIZE - 1024)) {
		if (medmysql_flush_medlist(&batches->acc_backup))
			return -1;
	}

	if (batches->acc_backup.len == 0)
		batches->acc_backup.len = sprintf(batches->acc_backup.str, "insert into acc_backup (method, from_tag, to_tag, callid, sip_code, sip_reason, time, time_hires, src_leg, dst_leg, dst_user, dst_ouser, dst_domain, src_user, src_domain) select method, from_tag, to_tag, callid, sip_code, sip_reason, time, time_hires, src_leg, dst_leg, dst_user, dst_ouser, dst_domain, src_user, src_domain from acc where callid in (");

	batches->acc_backup.len += sprintf(batches->acc_backup.str + batches->acc_backup.len, "'%s',", callid);

	return medmysql_delete_entries(callid, batches);
}


/**********************************************************************/
int medmysql_delete_entries(const char *callid, struct medmysql_batches *batches)
{
	if (batches->to_delete.len > (PACKET_SIZE - 1024)) {
		if (medmysql_flush_medlist(&batches->acc_backup))
			return -1;
		if (medmysql_flush_medlist(&batches->acc_trash))
			return -1;
		if (medmysql_flush_medlist(&batches->to_delete))
			return -1;
	}

	if (batches->to_delete.len == 0)
		batches->to_delete.len = sprintf(batches->to_delete.str, "delete from acc where callid in (");

	batches->to_delete.len += sprintf(batches->to_delete.str + batches->to_delete.len, "'%s',", callid);

	return 0;
}

#define CDRPRINT(x)	batches->cdrs.len += sprintf(batches->cdrs.str + batches->cdrs.len, x)
#define CDRESCAPE(x)	batches->cdrs.len += mysql_real_escape_string(med_handler->m, batches->cdrs.str + batches->cdrs.len, x, strlen(x))

/**********************************************************************/
int medmysql_insert_cdrs(cdr_entry_t *entries, uint64_t count, struct medmysql_batches *batches)
{
	uint64_t i;
	int gpp;

	for(i = 0; i < count; ++i)
	{
		if (batches->cdrs.len > (PACKET_SIZE - 6000)) {
			if (medmysql_flush_cdr(batches))
				return -1;
		}

		if (batches->cdrs.len == 0) {
			batches->cdrs.len = sprintf(batches->cdrs.str, "insert into cdr (id, update_time, " \
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
					") values ");
		}

		cdr_entry_t *e = &(entries[i]);
		char str_source_clir[2] = "";
		char str_split[2] = "";
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

		CDRPRINT("(NULL, now(), '");
		CDRESCAPE(e->source_user_id);
		CDRPRINT("','");
		CDRESCAPE(e->source_provider_id);
		CDRPRINT("','");

		CDRESCAPE(e->source_ext_subscriber_id);
		CDRPRINT("','");
		CDRESCAPE(e->source_ext_contract_id);
		CDRPRINT("',");
		CDRESCAPE(str_source_accid);
		CDRPRINT(",'");

		CDRESCAPE(e->source_user);
		CDRPRINT("','");
		CDRESCAPE(e->source_domain);
		CDRPRINT("','");
		CDRESCAPE(e->source_cli);
		CDRPRINT("',");
		CDRESCAPE(str_source_clir);
		CDRPRINT(",'");
		CDRESCAPE(e->source_ip);
		CDRPRINT("','");
		CDRESCAPE(e->destination_user_id);
		CDRPRINT("','");
		CDRESCAPE(e->destination_provider_id);
		CDRPRINT("','");
		CDRESCAPE(e->destination_ext_subscriber_id);
		CDRPRINT("','");
		CDRESCAPE(e->destination_ext_contract_id);
		CDRPRINT("',");
		CDRESCAPE(str_dest_accid);
		CDRPRINT(",'");
		CDRESCAPE(e->destination_user);
		CDRPRINT("','");
		CDRESCAPE(e->destination_domain);
		CDRPRINT("','");
		CDRESCAPE(e->destination_user_in);
		CDRPRINT("','");
		CDRESCAPE(e->destination_domain_in);
		CDRPRINT("','");
		CDRESCAPE(e->destination_dialed);
		CDRPRINT("','");
		CDRESCAPE(e->peer_auth_user);
		CDRPRINT("','");
		CDRESCAPE(e->peer_auth_realm);
		CDRPRINT("','");
		CDRESCAPE(e->call_type);
		CDRPRINT("','");
		CDRESCAPE(e->call_status);
		CDRPRINT("','");
		CDRESCAPE(e->call_code);
		CDRPRINT("',");
		CDRESCAPE(str_init_time);
		CDRPRINT(",");
		CDRESCAPE(str_start_time);
		CDRPRINT(",");
		CDRESCAPE(str_duration);
		CDRPRINT(",'");
		CDRESCAPE(e->call_id);
		CDRPRINT("',");
		CDRESCAPE(str_source_carrier_cost);
		CDRPRINT(",");
		CDRESCAPE(str_source_reseller_cost);
		CDRPRINT(",");
		CDRESCAPE(str_source_customer_cost);
		CDRPRINT(",");
		CDRESCAPE(str_dest_carrier_cost);
		CDRPRINT(",");
		CDRESCAPE(str_dest_reseller_cost);
		CDRPRINT(",");
		CDRESCAPE(str_dest_customer_cost);
		CDRPRINT(",");
		CDRESCAPE(str_split);
		for(gpp = 0; gpp < 10; ++gpp)
		{
			if(strnlen(e->source_gpp[gpp], sizeof(e->source_gpp[gpp])) > 0)
			{
				CDRPRINT(",'");
				CDRESCAPE(e->source_gpp[gpp]);
				CDRPRINT("'");
			}
			else
			{
				CDRPRINT(",NULL");
			}
		}
		for(gpp = 0; gpp < 10; ++gpp)
		{
			if(strnlen(e->destination_gpp[gpp], sizeof(e->destination_gpp[gpp])) > 0)
			{
				CDRPRINT(",'");
				CDRESCAPE(e->destination_gpp[gpp]);
				CDRPRINT("'");
			}
			else
			{
				CDRPRINT(",NULL");
			}
		}

		CDRPRINT(",'");
		CDRESCAPE(e->source_lnp_prefix);
		CDRPRINT("','");
		CDRESCAPE(e->destination_lnp_prefix);
		CDRPRINT("','");
		CDRESCAPE(e->source_user_out);
		CDRPRINT("','");
		CDRESCAPE(e->destination_user_out);
		CDRPRINT("'");

		if(strnlen(e->source_lnp_type, sizeof(e->source_lnp_type)) > 0)
		{
			CDRPRINT(",'");
			CDRESCAPE(e->source_lnp_type);
			CDRPRINT("'");
		}
		else
		{
			CDRPRINT(",NULL");
		}

		if(strnlen(e->destination_lnp_type, sizeof(e->destination_lnp_type)) > 0)
		{
			CDRPRINT(",'");
			CDRESCAPE(e->destination_lnp_type);
			CDRPRINT("'");
		}
		else
		{
			CDRPRINT(",NULL");
		}

		CDRPRINT("),");

		// no check for return codes here we should keep on nevertheless
		medmysql_update_call_stat_info(e->call_code, e->start_time, batches);

		if (check_shutdown())
			return -1;
	}

	/*syslog(LOG_DEBUG, "q='%s'", query);*/


	return 0;
}

/**********************************************************************/
int medmysql_update_call_stat_info(const char *call_code, const double start_time, struct medmysql_batches *batches)
{
	if (!med_call_stat_info_table)
		return -1;

	char period[STAT_PERIOD_SIZE];
	time_t etime = (time_t)start_time;
	char period_key[STAT_PERIOD_SIZE+4];
	struct medmysql_call_stat_info_t * period_t;

	switch (config_stats_period)
	{
		case MED_STATS_HOUR:
			strftime(period, STAT_PERIOD_SIZE, "%Y-%m-%d %H:00:00", localtime(&etime));
			break;
		case MED_STATS_DAY:
			strftime(period, STAT_PERIOD_SIZE, "%Y-%m-%d 00:00:00", localtime(&etime));
			break;
		case MED_STATS_MONTH:
			strftime(period, STAT_PERIOD_SIZE, "%Y-%m-01 00:00:00", localtime(&etime));
			break;
		default:
			syslog(LOG_CRIT, "Undefinied or wrong config_stats_period %d",
					config_stats_period);
			return -1;
	}

	sprintf(period_key, "%s-%s", period, call_code);

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

	/* syslog(LOG_DEBUG, "q='%s'", query); */
	if(medmysql_query_wrapper(prov_handler, MED_LOAD_PEER_QUERY, strlen(MED_LOAD_PEER_QUERY)) != 0)
	{
		syslog(LOG_CRIT, "Error loading peer hosts: %s",
				mysql_error(prov_handler->m));
		return -1;
	}

	res = mysql_store_result(prov_handler->m);

	while((row = mysql_fetch_row(res)) != NULL)
	{
		if(row[0] == NULL || row[2] == NULL)
		{
			syslog(LOG_CRIT, "Error loading peer hosts, a mandatory column is NULL");
			ret = -1;
			goto out;
		}

		if(ip_table != NULL)
		{
			if(g_hash_table_lookup(ip_table, row[0]) != NULL)
				syslog(LOG_WARNING, "Skipping duplicate IP '%s'", row[0]);
			else
				g_hash_table_insert(ip_table, strdup(row[0]), strdup(row[2]));
		}
		if(host_table != NULL && row[1] != NULL) // host column is optional
		{
			if(g_hash_table_lookup(host_table, row[1]) != NULL)
				syslog(LOG_WARNING, "Skipping duplicate host '%s'", row[1]);
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
int medmysql_load_uuids(GHashTable *uuid_table)
{
	MYSQL_RES *res;
	MYSQL_ROW row;
	int ret = 0;
	/* char query[1024] = ""; */
	gpointer key;
	char *provider_id;

	/* snprintf(query, sizeof(query), MED_LOAD_UUID_QUERY); */

	/* syslog(LOG_DEBUG, "q='%s'", query); */
	if(medmysql_query_wrapper(prov_handler, MED_LOAD_UUID_QUERY, strlen(MED_LOAD_UUID_QUERY)) != 0)
	{
		syslog(LOG_CRIT, "Error loading uuids: %s",
				mysql_error(prov_handler->m));
		return -1;
	}

	res = mysql_store_result(prov_handler->m);

	while((row = mysql_fetch_row(res)) != NULL)
	{
		if(row[0] == NULL || row[1] == NULL)
		{
			syslog(LOG_CRIT, "Error loading uuids, a column is NULL");
			ret = -1;
			goto out;
		}

		provider_id = strdup(row[1]);
		if(provider_id == NULL)
		{
			syslog(LOG_CRIT, "Error allocating provider id memory: %s", strerror(errno));
			ret = -1;
			goto out;
		}

		key = (gpointer)g_strdup(row[0]);
		g_hash_table_insert(uuid_table, key, provider_id);
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


int medmysql_batch_start(struct medmysql_batches *batches) {
	if (medmysql_handler_transaction(cdr_handler))
		return -1;
	if (medmysql_handler_transaction(med_handler))
		return -1;

	if (!med_call_stat_info_table)
		med_call_stat_info_table = g_hash_table_new_full(g_str_hash, g_str_equal, free, free);

	batches->cdrs.len = 0;
	batches->acc_backup.len = 0;
	batches->acc_trash.len = 0;
	batches->to_delete.len = 0;

	return 0;
}


static int medmysql_flush_cdr(struct medmysql_batches *batches) {
	//FILE *qlog;

	if (batches->cdrs.len == 0)
		return 0;
	if (batches->cdrs.str[batches->cdrs.len - 1] != ',')
		return 0;

	batches->cdrs.len--;
	batches->cdrs.str[batches->cdrs.len] = '\0';

	if(medmysql_query_wrapper_tx(cdr_handler, batches->cdrs.str, batches->cdrs.len) != 0)
	{
		batches->cdrs.len = 0;
		syslog(LOG_CRIT, "Error inserting cdrs: %s",
				mysql_error(cdr_handler->m));

		// agranig: tmp. way to log failed query, since it's too big
		// for syslog. Make this configurable (path, enable) via
		// cmd line switches
		//qlog = fopen("/var/log/ngcp/cdr-query.log", "a");
		//if(qlog == NULL)
		//{
		//	syslog(LOG_CRIT, "Failed to open cdr query log file '/var/log/ngcp/cdr-query.log': %s", strerror(errno));
		//	return -1;
		//}
		//if(fputs(batches->cdrs.str, qlog) == EOF)
		//{
		//	syslog(LOG_CRIT, "Failed to write to cdr query log file '/var/log/ngcp/cdr-query.log': %s", strerror(errno));
		//}
		//fclose(qlog);

		return -1;
	}

	batches->cdrs.len = 0;
	return 0;
}

static int medmysql_flush_medlist(struct medmysql_str *str) {
	if (str->len == 0)
		return 0;
	if (str->str[str->len - 1] != ',')
		return 0;

	str->str[str->len - 1] = ')';

	if(medmysql_query_wrapper_tx(med_handler, str->str, str->len) != 0)
	{
		str->len = 0;
		syslog(LOG_CRIT, "Error executing query: %s",
				mysql_error(med_handler->m));
		critical("Failed to execute potentially crucial SQL query, check syslog for details");
		return -1;
	}

	str->len = 0;
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
			syslog(LOG_CRIT,
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

		//syslog(LOG_DEBUG, "updating call stats info: %s -- %s", period_t->call_code, period_t->period);
		//syslog(LOG_DEBUG, "sql: %s", query.str);

		if(medmysql_query_wrapper(stats_handler, query.str, query.len) != 0)
		{
			syslog(LOG_CRIT, "Error executing call info stats query: %s",
					mysql_error(stats_handler->m));
			syslog(LOG_CRIT, "stats query: %s", query.str);
			critical("Failed to execute potentially crucial SQL query, check syslog for details");
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
	if (medmysql_flush_medlist(&batches->acc_trash) || check_shutdown())
		return -1;
	if (medmysql_flush_medlist(&batches->acc_backup) || check_shutdown())
		return -1;
	if (medmysql_flush_medlist(&batches->to_delete) || check_shutdown())
		return -1;
	if (medmysql_flush_call_stat_info() || check_shutdown())
		return -1;

	if (medmysql_handler_commit(cdr_handler))
		return -1;
	if (medmysql_handler_commit(med_handler))
		return -1;

	return 0;
}
