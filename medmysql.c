#include <my_global.h>
#include <m_string.h>
#include <mysql.h>

#include "medmysql.h"
#include "config.h"

/*#define MED_CALLID_QUERY "(select a.callid, a.time from acc a, acc b where a.callid = b.callid and a.method = 'INVITE' and b.method = 'BYE' group by callid) union (select callid, time from acc where method = 'INVITE' and sip_code != '200') order by time asc limit 0,200000"*/
#define MED_CALLID_QUERY "select a.callid from acc a left join acc b on a.callid = b.callid and b.method = 'BYE' where a.method = 'INVITE' and (a.sip_code != '200' or b.id is not null) group by a.callid limit 0,200000"

#define MED_FETCH_QUERY "select sip_code, sip_reason, method, callid, time, unix_timestamp(time), " \
	"src_leg, dst_leg, id " \
	"from acc where callid = '%s' order by time asc"

#define MED_LOAD_PEER_QUERY "select h.domain, h.ip, g.peering_contract_id " \
	"from provisioning.voip_peer_hosts h, provisioning.voip_peer_groups g " \
	"where g.id = h.group_id"
#define MED_LOAD_UUID_QUERY "select vs.uuid, r.contract_id from billing.voip_subscribers vs, " \
	"billing.contracts c, billing.resellers r where c.id = vs.contract_id and " \
	"c.reseller_id = r.id"

static MYSQL *cdr_handler = NULL;
static MYSQL *med_handler = NULL;
static MYSQL *prov_handler = NULL;

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

	if(mysql_real_query(med_handler, query, strlen(query)) != 0)
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
	memset(*callids, 0, callid_size);
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
			strcpy(c->value, "0");
		}
		strcpy(c->value, row[0]);

		/*syslog(LOG_DEBUG, "callid[%"PRIu64"]='%s'", i, c->value);*/
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

	if(mysql_real_query(med_handler, query, strlen(query)) != 0)
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
		e->unix_timestamp = atoll(row[5]);
		g_strlcpy(e->src_leg, row[6], sizeof(e->src_leg));
		g_strlcpy(e->dst_leg, row[7], sizeof(e->dst_leg));
		e->med_id = atoll(row[8]);
		e->valid = 1;
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
		batches->acc_trash.len = sprintf(batches->acc_trash.str, "insert into acc_trash select * from acc where callid in (");

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
		batches->acc_backup.len = sprintf(batches->acc_backup.str, "insert into acc_backup select * from acc where callid in (");

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

	for(i = 0; i < count; ++i)
	{
		if (batches->cdrs.len > (PACKET_SIZE - 6000)) {
			if (medmysql_flush_cdr(batches))
				return -1;
		}

		if (batches->cdrs.len == 0) {
			batches->cdrs.len = sprintf(batches->cdrs.str, "insert into cdr (id, update_time, " \
					"source_user_id, source_provider_id, source_user, source_domain, " \
					"source_cli, source_clir, "\
					"destination_user_id, destination_provider_id, destination_user, destination_domain, " \
					"destination_user_in, destination_domain_in, destination_user_dialed, " \
					"call_type, call_status, call_code, start_time, duration, call_id, " \
					"carrier_cost, reseller_cost, customer_cost) values ");
		}

		cdr_entry_t *e = &(entries[i]);
		char str_source_clir[2] = "";
		char str_duration[32] = "";
		char str_carrier_cost[32] = "";
		char str_reseller_cost[32] = "";
		char str_customer_cost[32] = "";
		snprintf(str_source_clir, sizeof(str_source_clir), "%d", e->source_clir);
		snprintf(str_duration, sizeof(str_duration), "%d", e->duration);
		snprintf(str_carrier_cost, sizeof(str_carrier_cost), "%d", e->carrier_cost);
		snprintf(str_reseller_cost, sizeof(str_reseller_cost), "%d", e->reseller_cost);
		snprintf(str_customer_cost, sizeof(str_customer_cost), "%d", e->customer_cost);

		CDRPRINT("(NULL, now(), '");
		CDRESCAPE(e->source_user_id);
		CDRPRINT("','");
		CDRESCAPE(e->source_provider_id);
		CDRPRINT("','");
		CDRESCAPE(e->source_user);
		CDRPRINT("','");
		CDRESCAPE(e->source_domain);
		CDRPRINT("','");
		CDRESCAPE(e->source_cli);
		CDRPRINT("',");
		CDRESCAPE(str_source_clir);
		CDRPRINT(",'");
		CDRESCAPE(e->destination_user_id);
		CDRPRINT("','");
		CDRESCAPE(e->destination_provider_id);
		CDRPRINT("','");
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
		CDRESCAPE(e->call_type);
		CDRPRINT("','");
		CDRESCAPE(e->call_status);
		CDRPRINT("','");
		CDRESCAPE(e->call_code);
		CDRPRINT("','");
		CDRESCAPE(e->start_time);
		CDRPRINT("',");
		CDRESCAPE(str_duration);
		CDRPRINT(",'");
		CDRESCAPE(e->call_id);
		CDRPRINT("',");
		CDRESCAPE(str_carrier_cost);
		CDRPRINT(",");
		CDRESCAPE(str_reseller_cost);
		CDRPRINT(",");
		CDRESCAPE(str_customer_cost);
		CDRPRINT("),");
	}
	
	/*syslog(LOG_DEBUG, "q='%s'", query);*/
	
	
	return 0;
}

/**********************************************************************/
int medmysql_load_maps(GHashTable *host_table, GHashTable *ip_table)
{
	MYSQL_RES *res;
	MYSQL_ROW row;
	int ret = 0;
	char query[1024] = "";
	gpointer key;
	char* host_id;
	char* ip_id;

	snprintf(query, sizeof(query), MED_LOAD_PEER_QUERY);

	/* syslog(LOG_DEBUG, "q='%s'", query); */
	if(mysql_real_query(prov_handler, query, strlen(query)) != 0)
	{
		syslog(LOG_CRIT, "Error loading peer hosts: %s", 
				mysql_error(prov_handler));
		return -1;
	}

	res = mysql_store_result(prov_handler);

	while((row = mysql_fetch_row(res)) != NULL)
	{
		host_id = NULL;
		ip_id = NULL;

		if(row[0] == NULL || row[1] == NULL || row[2] == NULL)
		{
			syslog(LOG_CRIT, "Error loading peer hosts, a column is NULL");
			ret = -1;
			goto out;
		}

		host_id = strdup(row[2]);
		if(host_id == NULL)
		{
			syslog(LOG_CRIT, "Error allocating host id memory: %s", strerror(errno));
			ret = -1;
			goto out;
		}

		ip_id = strdup(row[2]);
		if(ip_id == NULL)
		{
			syslog(LOG_CRIT, "Error allocating ip id memory: %s", strerror(errno));
			free(host_id);
			ret = -1;
			goto out;
		}

		if(g_hash_table_lookup(host_table, row[0]) != NULL)
		{
			syslog(LOG_WARNING, "Skipping duplicate hostname '%s'", row[0]);
		}
		else
		{
			key = (gpointer)g_strdup(row[0]);
			g_hash_table_insert(host_table, key, host_id);
		}
	
		if(ip_table != NULL)
		{
			if(g_hash_table_lookup(ip_table, row[1]) != NULL)
			{
				syslog(LOG_WARNING, "Skipping duplicate IP '%s'", row[1]);
			}
			else
			{
				key = (gpointer)g_strdup(row[1]);
				g_hash_table_insert(ip_table, key, ip_id);
			}
		}
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
	if(mysql_real_query(prov_handler, query, strlen(query)) != 0)
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


void medmysql_batch_start(struct medmysql_batches *batches) {
	mysql_real_query(cdr_handler, "start transaction", 17);
	mysql_real_query(med_handler, "start transaction", 17);

	batches->cdrs.len = 0;
	batches->acc_backup.len = 0;
	batches->acc_trash.len = 0;
	batches->to_delete.len = 0;
}


int medmysql_flush_cdr(struct medmysql_batches *batches) {
	if (batches->cdrs.len == 0)
		return 0;
	if (batches->cdrs.str[batches->cdrs.len - 1] != ',')
		return 0;

	batches->cdrs.len--;
	batches->cdrs.str[batches->cdrs.len] = '\0';

	if(mysql_real_query(cdr_handler, batches->cdrs.str, batches->cdrs.len) != 0)
	{
		batches->cdrs.len = 0;
		syslog(LOG_CRIT, "Error inserting cdrs: %s", 
				mysql_error(cdr_handler));
		return -1;
	}

	batches->cdrs.len = 0;
	return 0;
}

int medmysql_flush_medlist(struct medmysql_str *str) {
	if (str->len == 0)
		return 0;
	if (str->str[str->len - 1] != ',')
		return 0;

	str->str[str->len - 1] = ')';

	if(mysql_real_query(med_handler, str->str, str->len) != 0)
	{
		str->len = 0;
		syslog(LOG_CRIT, "Error executing query: %s", 
				mysql_error(cdr_handler));
		return -1;
	}

	str->len = 0;
	return 0;
}

void medmysql_batch_end(struct medmysql_batches *batches) {
	medmysql_flush_cdr(batches);
	medmysql_flush_medlist(&batches->acc_trash);
	medmysql_flush_medlist(&batches->acc_backup);
	medmysql_flush_medlist(&batches->to_delete);

	mysql_real_query(cdr_handler, "commit", 6);
	mysql_real_query(med_handler, "commit", 6);
}
