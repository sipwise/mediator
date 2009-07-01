#include <my_global.h>
#include <m_string.h>
#include <mysql.h>

#include "medmysql.h"
#include "config.h"

#define MED_CALLID_QUERY "(select a.callid, a.time from acc a, acc b where a.callid = b.callid and a.method = 'INVITE' and b.method = 'BYE' group by callid) union (select callid, time from acc where method = 'INVITE' and sip_code != '200') order by time asc limit 0,200000"

#define MED_FETCH_QUERY "select sip_code, sip_reason, method, callid, time, unix_timestamp(time), " \
	"src_leg, dst_leg, id " \
	"from acc where callid = '%s' order by time asc"

#define MED_TRASH_QUERY "insert into acc_trash select * from acc where callid = '%s'"
#define MED_BACKUP_QUERY "insert into acc_backup select * from acc where callid = '%s'"
#define MED_DELETE_QUERY "delete from acc where callid = '%s'"

#define MED_LOAD_PEER_QUERY "select domain, ip, provider_id from provisioning.voip_peer_hosts"
#define MED_LOAD_UUID_QUERY "select vs.uuid, r.contract_id from billing.voip_subscribers vs, " \
	"billing.contracts c, billing.resellers r where c.id = vs.contract_id and " \
	"c.reseller_id = r.id"

static MYSQL *cdr_handler = NULL;
static MYSQL *med_handler = NULL;

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

	strncpy(query, MED_CALLID_QUERY, sizeof(query));
	
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

		strncpy(e->sip_code, row[0], sizeof(e->sip_code));
		strncpy(e->sip_reason, row[1], sizeof(e->sip_reason));
		strncpy(e->sip_method, row[2], sizeof(e->sip_method));
		strncpy(e->callid, row[3], sizeof(e->callid));
		strncpy(e->timestamp, row[4], sizeof(e->timestamp));
		e->unix_timestamp = atoll(row[5]);
		strncpy(e->src_leg, row[6], sizeof(e->src_leg));
		strncpy(e->dst_leg, row[7], sizeof(e->dst_leg));
		e->med_id = atoll(row[8]);
		e->valid = 1;
	}

out:
	mysql_free_result(res);
	return ret;
}

/**********************************************************************/
int medmysql_trash_entries(const char *callid)
{
	char query[1024] = "";

	snprintf(query, sizeof(query), MED_TRASH_QUERY, callid);

	/* syslog(LOG_DEBUG, "q='%s'", query); */
	
	if(mysql_real_query(med_handler, query, strlen(query)) != 0)
	{
		syslog(LOG_CRIT, "Error moving entries for callid '%s' to trash: %s", 
				callid, mysql_error(med_handler));
		return -1;
	}

	return medmysql_delete_entries(callid);
}

/**********************************************************************/
int medmysql_backup_entries(const char *callid)
{
	char query[1024] = "";

	snprintf(query, sizeof(query), MED_BACKUP_QUERY, callid);

	/* syslog(LOG_DEBUG, "q='%s'", query); */
	
	if(mysql_real_query(med_handler, query, strlen(query)) != 0)
	{
		syslog(LOG_CRIT, "Error moving entries for callid '%s' to backup: %s", 
				callid, mysql_error(med_handler));
		return -1;
	}

	return medmysql_delete_entries(callid);
}


/**********************************************************************/
int medmysql_delete_entries(const char *callid)
{
	char query[1024] = "";

	snprintf(query, sizeof(query), MED_DELETE_QUERY, callid);

	/* syslog(LOG_DEBUG, "q='%s'", query); */
	
	if(mysql_real_query(med_handler, query, strlen(query)) != 0)
	{
		syslog(LOG_CRIT, "Error deleting entries for callid '%s' from acc: %s", 
				callid, mysql_error(med_handler));
		return -1;
	}

	return 0;
}

/**********************************************************************/
int medmysql_insert_cdrs(cdr_entry_t *entries, u_int64_t count)
{
	size_t q_size;
	char *query, *end;
	u_int64_t i;
	int ret = 0;

	q_size = 128 + (sizeof(cdr_entry_t) * 2 + 64) * count;
	query = (char*)malloc(q_size);
	if(query == NULL)
	{
		syslog(LOG_CRIT, "Error allocating cdr query memory: %s", strerror(errno));
		return -1;
	}

	end = strmov(query, "insert into cdr (id, update_time, " \
			"source_user_id, source_provider_id, source_user, source_domain, " \
			"source_cli, source_clir, "\
			"destination_user_id, destination_provider_id, destination_user, destination_domain, " \
			"destination_user_in, destination_domain_in, destination_user_dialed, " \
			"call_type, call_status, call_code, start_time, duration, call_id, " \
			"carrier_cost, reseller_cost, customer_cost) values ");

	for(i = 0; i < count; ++i)
	{
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

		end = strmov(end, "(NULL, now(), '");
		end += mysql_real_escape_string(med_handler, end, e->source_user_id, strlen(e->source_user_id));
		end = strmov(end, "','");
		end += mysql_real_escape_string(med_handler, end, e->source_provider_id, strlen(e->source_provider_id));
		end = strmov(end, "','");
		end += mysql_real_escape_string(med_handler, end, e->source_user, strlen(e->source_user));
		end = strmov(end, "','");
		end += mysql_real_escape_string(med_handler, end, e->source_domain, strlen(e->source_domain));
		end = strmov(end, "','");
		end += mysql_real_escape_string(med_handler, end, e->source_cli, strlen(e->source_cli));
		end = strmov(end, "',");
		end += mysql_real_escape_string(med_handler, end, str_source_clir, strlen(str_source_clir));
		end = strmov(end, ",'");
		end += mysql_real_escape_string(med_handler, end, e->destination_user_id, strlen(e->destination_user_id));
		end = strmov(end, "','");
		end += mysql_real_escape_string(med_handler, end, e->destination_provider_id, strlen(e->destination_provider_id));
		end = strmov(end, "','");
		end += mysql_real_escape_string(med_handler, end, e->destination_user, strlen(e->destination_user));
		end = strmov(end, "','");
		end += mysql_real_escape_string(med_handler, end, e->destination_domain, strlen(e->destination_domain));
		end = strmov(end, "','");
		end += mysql_real_escape_string(med_handler, end, e->destination_user_in, strlen(e->destination_user_in));
		end = strmov(end, "','");
		end += mysql_real_escape_string(med_handler, end, e->destination_domain_in, strlen(e->destination_domain_in));
		end = strmov(end, "','");
		end += mysql_real_escape_string(med_handler, end, e->destination_dialed, strlen(e->destination_dialed));
		end = strmov(end, "','");
		end += mysql_real_escape_string(med_handler, end, e->call_type, strlen(e->call_type));
		end = strmov(end, "','");
		end += mysql_real_escape_string(med_handler, end, e->call_status, strlen(e->call_status));
		end = strmov(end, "','");
		end += mysql_real_escape_string(med_handler, end, e->call_code, strlen(e->call_code));
		end = strmov(end, "','");
		end += mysql_real_escape_string(med_handler, end, e->start_time, strlen(e->start_time));
		end = strmov(end, "',");
		end += mysql_real_escape_string(med_handler, end, str_duration, strlen(str_duration));
		end = strmov(end, ",'");
		end += mysql_real_escape_string(med_handler, end, e->call_id, strlen(e->call_id));
		end = strmov(end, "',");
		end += mysql_real_escape_string(med_handler, end, str_carrier_cost, strlen(str_carrier_cost));
		end = strmov(end, ",");
		end += mysql_real_escape_string(med_handler, end, str_reseller_cost, strlen(str_reseller_cost));
		end = strmov(end, ",");
		end += mysql_real_escape_string(med_handler, end, str_customer_cost, strlen(str_customer_cost));
		end = strmov(end, "),");
	}
	query[strlen(query)-1] = '\0';
	
	/*syslog(LOG_DEBUG, "q='%s'", query);*/
	
	if(mysql_real_query(cdr_handler, query, strlen(query)) != 0)
	{
		syslog(LOG_CRIT, "Error inserting cdrs: %s", 
				mysql_error(cdr_handler));
		ret = -1;
		goto out;
	}
	
out:	
	free(query);
	return ret;
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
	if(mysql_real_query(med_handler, query, strlen(query)) != 0)
	{
		syslog(LOG_CRIT, "Error loading peer hosts: %s", 
				mysql_error(med_handler));
		return -1;
	}

	res = mysql_store_result(med_handler);

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
	if(mysql_real_query(med_handler, query, strlen(query)) != 0)
	{
		syslog(LOG_CRIT, "Error loading uuids: %s", 
				mysql_error(med_handler));
		return -1;
	}

	res = mysql_store_result(med_handler);

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


