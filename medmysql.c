#include <my_global.h>
#include <m_string.h>
#include <mysql.h>
#include <mysql/errmsg.h>

#include "medmysql.h"
#include "config.h"

/*#define MED_CALLID_QUERY "(select a.callid, a.time from acc a, acc b where a.callid = b.callid and a.method = 'INVITE' and b.method = 'BYE' group by callid) union (select callid, time from acc where method = 'INVITE' and sip_code != '200') order by time asc limit 0,200000"*/
#define MED_CALLID_QUERY "select a.callid from acc a left join acc b on a.callid = b.callid and b.method = 'BYE' where a.method = 'INVITE' and (a.sip_code != '200' or b.id is not null) group by a.callid limit 0,200000"

#define MED_FETCH_QUERY "select distinct sip_code, sip_reason, method, callid, time, time_hires, " \
	"src_leg, dst_leg " \
	"from acc where callid = '%s' order by time_hires asc"

#define MED_LOAD_PEER_QUERY "select h.ip, h.host, g.peering_contract_id, h.id " \
	"from provisioning.voip_peer_hosts h, provisioning.voip_peer_groups g " \
	"where g.id = h.group_id"
#define MED_LOAD_UUID_QUERY "select vs.uuid, r.contract_id from billing.voip_subscribers vs, " \
	"billing.contracts c, billing.contacts ct, billing.resellers r where c.id = vs.contract_id and " \
	"c.contact_id = ct.id and ct.reseller_id = r.id"

static MYSQL *cdr_handler = NULL;
static MYSQL *med_handler = NULL;
static MYSQL *prov_handler = NULL;
static MYSQL *stats_handler = NULL;

static int medmysql_flush_cdr(struct medmysql_batches *);
static int medmysql_flush_medlist(struct medmysql_str *);
static int medmysql_flush_call_stat_info();


static int mysql_query_wrapper(MYSQL *mysql, const char *stmt_str, unsigned long length) {
	int ret;
	int i;
	unsigned int err;

	for (i = 0; i < 10; i++) {
		ret = mysql_real_query(mysql, stmt_str, length);
		if (!ret)
			return ret;
		err = mysql_errno(mysql);
		if (err == CR_SERVER_GONE_ERROR || err == CR_SERVER_LOST || err == CR_CONN_HOST_ERROR || err == CR_CONNECTION_ERROR || err == CR_SERVER_LOST) {
			syslog(LOG_WARNING, "Lost connection to SQL server during query, retrying...");
			sleep(10);
			continue;
		}
		break;
	}
	return ret;
}

/**********************************************************************/
int medmysql_init()
{
	my_bool recon = 1;

	cdr_handler = mysql_init(NULL);
	if(!mysql_real_connect(cdr_handler, 
				config_cdr_host, config_cdr_user, config_cdr_pass,
				config_cdr_db, config_cdr_port, NULL, 0))
	{
		syslog(LOG_CRIT, "Error connecting to CDR db: %s", mysql_error(cdr_handler));
		goto err;
	}
	if(mysql_options(cdr_handler, MYSQL_OPT_RECONNECT, &recon) != 0)
	{
		syslog(LOG_CRIT, "Error setting reconnect-option for CDR db: %s", mysql_error(cdr_handler));
		goto err;
	}


	med_handler = mysql_init(NULL);
	if(!mysql_real_connect(med_handler, 
				config_med_host, config_med_user, config_med_pass,
				config_med_db, config_med_port, NULL, 0))
	{
		syslog(LOG_CRIT, "Error connecting to ACC db: %s", mysql_error(med_handler));
		goto err;
	}
	if(mysql_options(med_handler, MYSQL_OPT_RECONNECT, &recon) != 0)
	{
		syslog(LOG_CRIT, "Error setting reconnect-option for ACC db: %s", mysql_error(med_handler));
		goto err;
	}
	
	prov_handler = mysql_init(NULL);
	if(!mysql_real_connect(prov_handler, 
				config_prov_host, config_prov_user, config_prov_pass,
				config_prov_db, config_prov_port, NULL, 0))
	{
		syslog(LOG_CRIT, "Error connecting to provisioning db: %s", mysql_error(prov_handler));
		goto err;
	}
	if(mysql_options(prov_handler, MYSQL_OPT_RECONNECT, &recon) != 0)
	{
		syslog(LOG_CRIT, "Error setting reconnect-option for provisioning db: %s", mysql_error(prov_handler));
		goto err;
	}

	stats_handler = mysql_init(NULL);
	if(!mysql_real_connect(stats_handler,
				config_stats_host, config_stats_user, config_stats_pass,
				config_stats_db, config_stats_port, NULL, 0))
	{
		syslog(LOG_CRIT, "Error connecting to STATS db: %s", mysql_error(stats_handler));
		goto err;
	}
	if(mysql_options(stats_handler, MYSQL_OPT_RECONNECT, &recon) != 0)
	{
		syslog(LOG_CRIT, "Error setting reconnect-option for STATS db: %s", mysql_error(stats_handler));
		goto err;
	}

	return 0;

err:
	medmysql_cleanup();	
	return -1;
}

/**********************************************************************/
void medmysql_cleanup()
{
	if(cdr_handler != NULL)
	{
		mysql_close(cdr_handler);
		cdr_handler = NULL;
	}
	if(med_handler != NULL)
	{
		mysql_close(med_handler);
		med_handler = NULL;
	}
	if(prov_handler != NULL)
	{
		mysql_close(prov_handler);
		prov_handler = NULL;
	}
	if(stats_handler != NULL)
	{
		mysql_close(stats_handler);
		stats_handler = NULL;
	}
}

/**********************************************************************/
int medmysql_fetch_callids(med_callid_t **callids, u_int64_t *count)
{
	MYSQL_RES *res;
	MYSQL_ROW row;
	char query[1024] = "";
	size_t callid_size;
	u_int64_t i = 0;
	int ret = 0;

	*count = 0;

	g_strlcpy(query, MED_CALLID_QUERY, sizeof(query));
	
	/*syslog(LOG_DEBUG, "q='%s'", query);*/

	if(mysql_query_wrapper(med_handler, query, strlen(query)) != 0)
	{
		syslog(LOG_CRIT, "Error getting acc callids: %s", 
				mysql_error(med_handler));
		return -1;
	}

	res = mysql_store_result(med_handler);
	*count = mysql_num_rows(res);
	if(*count == 0)
	{
		goto out;
	}

	callid_size = sizeof(med_callid_t) * (*count);
	*callids = (med_callid_t*)malloc(callid_size);
	memset(*callids, '\0', callid_size);
	if(*callids == NULL)
	{
		syslog(LOG_CRIT, "Error allocating callid memory: %s", strerror(errno));
		ret = -1;
		goto out;
	}

	while((row = mysql_fetch_row(res)) != NULL)
	{
		med_callid_t *c = &(*callids)[i++];
		if(row == NULL || row[0] == NULL)
		{
			g_strlcpy(c->value, "0", sizeof(c->value));
		} else {
			g_strlcpy(c->value, row[0], sizeof(c->value));
		}

		/*syslog(LOG_DEBUG, "callid[%"PRIu64"]='%s'", i, c->value);*/

		if (check_shutdown())
			return -1;
	}
			
out:
	mysql_free_result(res);
	return ret;
}

/**********************************************************************/
int medmysql_fetch_records(med_callid_t *callid, 
		med_entry_t **entries, u_int64_t *count)
{
	MYSQL_RES *res;
	MYSQL_ROW row;
	char query[1024] = "";
	size_t entry_size;
	u_int64_t i = 0;
	int ret = 0;

	*count = 0;

	snprintf(query, sizeof(query), MED_FETCH_QUERY, callid->value);
	
	/*syslog(LOG_DEBUG, "q='%s'", query);*/

	if(mysql_query_wrapper(med_handler, query, strlen(query)) != 0)
	{
		syslog(LOG_CRIT, "Error getting acc records for callid '%s': %s", 
				callid->value, mysql_error(med_handler));
		return -1;
	}

	res = mysql_store_result(med_handler);
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
#define CDRESCAPE(x)	batches->cdrs.len += mysql_real_escape_string(med_handler, batches->cdrs.str + batches->cdrs.len, x, strlen(x))

/**********************************************************************/
int medmysql_insert_cdrs(cdr_entry_t *entries, u_int64_t count, struct medmysql_batches *batches)
{
	u_int64_t i;
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
                    "destination_gpp5, destination_gpp6, destination_gpp7, destination_gpp8, destination_gpp9" \
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
		strcpy(period_t->call_code, call_code);
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
	char query[1024] = "";

	snprintf(query, sizeof(query), MED_LOAD_PEER_QUERY);

	/* syslog(LOG_DEBUG, "q='%s'", query); */
	if(mysql_query_wrapper(prov_handler, query, strlen(query)) != 0)
	{
		syslog(LOG_CRIT, "Error loading peer hosts: %s", 
				mysql_error(prov_handler));
		return -1;
	}

	res = mysql_store_result(prov_handler);

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
	char query[1024] = "";
	gpointer key;
	char *provider_id;

	snprintf(query, sizeof(query), MED_LOAD_UUID_QUERY);

	/* syslog(LOG_DEBUG, "q='%s'", query); */
	if(mysql_query_wrapper(prov_handler, query, strlen(query)) != 0)
	{
		syslog(LOG_CRIT, "Error loading uuids: %s", 
				mysql_error(prov_handler));
		return -1;
	}

	res = mysql_store_result(prov_handler);

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


int medmysql_batch_start(struct medmysql_batches *batches) {
	if (mysql_query_wrapper(cdr_handler, "start transaction", 17))
		return -1;
	if (mysql_query_wrapper(med_handler, "start transaction", 17))
		return -1;
	if (mysql_query_wrapper(stats_handler, "start transaction", 17))
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
	/* FILE *qlog; */

	if (batches->cdrs.len == 0)
		return 0;
	if (batches->cdrs.str[batches->cdrs.len - 1] != ',')
		return 0;

	batches->cdrs.len--;
	batches->cdrs.str[batches->cdrs.len] = '\0';

	if(mysql_query_wrapper(cdr_handler, batches->cdrs.str, batches->cdrs.len) != 0)
	{
		batches->cdrs.len = 0;
		syslog(LOG_CRIT, "Error inserting cdrs: %s", 
				mysql_error(cdr_handler));
		/* 
		// agranig: tmp. way to log failed query, since it's too big
		// for syslog. Make this configurable (path, enable) via
		// cmd line switches
		qlog = fopen("/var/log/ngcp/cdr-query.log", "a");
		if(qlog == NULL)
		{
			syslog(LOG_CRIT, "Failed to open cdr query log file '/var/log/ngcp/cdr-query.log': %s", strerror(errno));
			return -1;
		}
		if(fputs(batches->cdrs.str, qlog) == EOF)
		{
			syslog(LOG_CRIT, "Failed to write to cdr query log file '/var/log/ngcp/cdr-query.log': %s", strerror(errno));
		}
		fclose(qlog);
		*/
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

	if(mysql_query_wrapper(med_handler, str->str, str->len) != 0)
	{
		str->len = 0;
		syslog(LOG_CRIT, "Error executing query: %s", 
				mysql_error(med_handler));
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

		if(mysql_query_wrapper(stats_handler, query.str, query.len) != 0)
		{
			syslog(LOG_CRIT, "Error executing call info stats query: %s",
					mysql_error(stats_handler));
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

	if (mysql_query_wrapper(cdr_handler, "commit", 6))
		return -1;
	if (mysql_query_wrapper(med_handler, "commit", 6))
		return -1;
	if (mysql_query_wrapper(stats_handler, "commit", 6))
		return -1;

	return 0;
}
