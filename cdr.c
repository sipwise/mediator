#include <ctype.h>

#include "cdr.h"
#include "medmysql.h"
#include "config.h"
#include "mediator.h"

static char* cdr_map_status(const char *sip_status)
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

int cdr_process_records(med_entry_t *records, u_int64_t count, u_int64_t *ext_count, struct medmysql_batches *batches)
{
	int ret = 0;
	u_int8_t trash = 0;
	u_int64_t i;

	u_int16_t msg_invites = 0;
	u_int16_t msg_byes = 0;
	u_int16_t msg_unknowns = 0;
	u_int16_t invite_200 = 0;

	char *callid = records[0].callid;


	cdr_entry_t *cdrs;
	u_int64_t cdr_count;

	*ext_count = 0;

	for(i = 0; i < count; ++i)
	{
		med_entry_t *e = &(records[i]);
		if(strcmp(e->sip_method, MSG_INVITE) == 0)
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
			++msg_unknowns;
			e->method = MED_UNRECOGNIZED;
		}

		if (check_shutdown())
			return -1;
	}
				
	/*syslog(LOG_DEBUG, "%d INVITEs, %d BYEs, %d unrecognized", msg_invites, msg_byes, msg_unknowns);*/
			
	if(msg_invites > 0)
	{
		if(msg_byes > 0 || invite_200 == 0)
		{
			if(/*msg_byes > 2*/ 0)
			{
				syslog(LOG_WARNING, "Multiple (%d) BYE messages for callid '%s' found, trashing...", 
						msg_byes, callid);
				trash = 1;
			}
			else
			{
				if(cdr_create_cdrs(records, count, &cdrs, &cdr_count, &trash) != 0)
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

						if(medmysql_insert_cdrs(cdrs, cdr_count, batches) != 0)
							goto error;
						else
						{
							if(medmysql_backup_entries(callid, batches) != 0)
								goto error;
						}
						
					}
					else
					{
						/* TODO: no CDRs created? */
					}
					free(cdrs);
				}
			}
		}
		else
		{
			/*
			syslog(LOG_DEBUG, "No BYE message for callid '%s' found, skipping...", 
					callid);
			*/
		}
	}
	else
	{
		/*syslog(LOG_WARNING, "No INVITE message for callid '%s' found, trashing...", callid);*/
		trash = 1;
	}

	if(trash)
	{
		if(medmysql_trash_entries(callid, batches) != 0)
			goto error;
	}
	return ret;


error:
	return -1;
}

static int cdr_parse_srcleg(char *srcleg, cdr_entry_t *cdr)
{
	char *tmp1, *tmp2;
	int len;

	len = strlen(srcleg);

	tmp2 = srcleg;
		
	tmp1 = strchr(tmp2, MED_SEP);
	if(tmp1 == NULL)
	{
		syslog(LOG_WARNING, "Call-Id '%s' has no separated source user id, '%s'", cdr->call_id, tmp2);
		return -1;
	}
	*tmp1 = '\0';
	g_strlcpy(cdr->source_user_id, tmp2, sizeof(cdr->source_user_id));
	tmp2 = ++tmp1;

	tmp1 = strchr(tmp2, MED_SEP);
	if(tmp1 == NULL)
	{
		syslog(LOG_WARNING, "Call-Id '%s' has no separated source user, '%s'", cdr->call_id, tmp2);
		return -1;
	}
	*tmp1 = '\0';
	g_strlcpy(cdr->source_user, tmp2, sizeof(cdr->source_user));
	tmp2 = ++tmp1;
	
	tmp1 = strchr(tmp2, MED_SEP);
	if(tmp1 == NULL)
	{
		syslog(LOG_WARNING, "Call-Id '%s' has no separated source domain, '%s'", cdr->call_id, tmp2);
		return -1;
	}
	*tmp1 = '\0';
	g_strlcpy(cdr->source_domain, tmp2, sizeof(cdr->source_domain));
	tmp2 = ++tmp1;
	

	tmp1 = strchr(tmp2, MED_SEP);
	if(tmp1 == NULL)
	{
		syslog(LOG_WARNING, "Call-Id '%s' has no separated source cli, '%s'", cdr->call_id, tmp2);
		return -1;
	}
	*tmp1 = '\0';
	g_strlcpy(cdr->source_cli, tmp2, sizeof(cdr->source_cli));
	tmp2 = ++tmp1;

	tmp1 = strchr(tmp2, MED_SEP);
	if(tmp1 == NULL)
	{
		syslog(LOG_WARNING, "Call-Id '%s' has no separated source external subscriber id, '%s'", cdr->call_id, tmp2);
		return -1;
	}
	*tmp1 = '\0';
	g_strlcpy(cdr->source_ext_subscriber_id, tmp2, sizeof(cdr->source_ext_subscriber_id));
	tmp2 = ++tmp1;

	tmp1 = strchr(tmp2, MED_SEP);
	if(tmp1 == NULL)
	{
		syslog(LOG_WARNING, "Call-Id '%s' has no separated source external contract id, '%s'", cdr->call_id, tmp2);
		return -1;
	}
	*tmp1 = '\0';
	g_strlcpy(cdr->source_ext_contract_id, tmp2, sizeof(cdr->source_ext_contract_id));
	tmp2 = ++tmp1;

	tmp1 = strchr(tmp2, MED_SEP);
	if(tmp1 == NULL)
	{
		syslog(LOG_WARNING, "Call-Id '%s' has no separated source account id, '%s'", cdr->call_id, tmp2);
		return -1;
	}
	*tmp1 = '\0';
	cdr->source_account_id = atoll(tmp2);
	tmp2 = ++tmp1;

	tmp1 = strchr(tmp2, MED_SEP);
	if(tmp1 == NULL)
	{
		syslog(LOG_WARNING, "Call-Id '%s' has no separated peer auth user, '%s'", cdr->call_id, tmp2);
		return -1;
	}
	*tmp1 = '\0';
	g_strlcpy(cdr->peer_auth_user, tmp2, sizeof(cdr->peer_auth_user));
	tmp2 = ++tmp1;

	tmp1 = strchr(tmp2, MED_SEP);
	if(tmp1 == NULL)
	{
		syslog(LOG_WARNING, "Call-Id '%s' has no separated peer auth realm, '%s'", cdr->call_id, tmp2);
		return -1;
	}
	*tmp1 = '\0';
	g_strlcpy(cdr->peer_auth_realm, tmp2, sizeof(cdr->peer_auth_realm));
	tmp2 = ++tmp1;

	tmp1 = strchr(tmp2, MED_SEP);
	if(tmp1 == NULL)
	{
		syslog(LOG_WARNING, "Call-Id '%s' has no separated source clir status, '%s'", cdr->call_id, tmp2);
		return -1;
	}
	*tmp1 = '\0';
	cdr->source_clir = atoi(tmp2);
	tmp2 = ++tmp1;

	tmp1 = strchr(tmp2, MED_SEP);
	if(tmp1 == NULL)
	{
		syslog(LOG_WARNING, "Call-Id '%s' has no separated call type, '%s'", cdr->call_id, tmp2);
		return -1;
	}
	*tmp1 = '\0';
	g_strlcpy(cdr->call_type, tmp2, sizeof(cdr->call_type));
	tmp2 = ++tmp1;

	tmp1 = strchr(tmp2, MED_SEP);
	if(tmp1 == NULL)
	{
		syslog(LOG_WARNING, "Call-Id '%s' has no separated source ip, '%s'", cdr->call_id, tmp2);
		return -1;
	}
	*tmp1 = '\0';
	g_strlcpy(cdr->source_ip, tmp2, sizeof(cdr->source_ip));
	tmp2 = ++tmp1;

	if(len < tmp2 - srcleg + 1)	
	{
		syslog(LOG_WARNING, "Call-Id '%s' has no separated init time, '%s'", cdr->call_id, tmp2);
		return -1;
	}
	cdr->init_time = g_strtod(tmp2, NULL);

	return 0;
}

static int cdr_parse_dstleg(char *dstleg, cdr_entry_t *cdr)
{
	char *tmp1, *tmp2;
	int len;
		
	len = strlen(dstleg);
	tmp2 = dstleg;

	tmp1 = strchr(tmp2, MED_SEP);
	if(tmp1 == NULL)
	{
		syslog(LOG_WARNING, "Call-Id '%s' has no separated split flag, '%s'", cdr->call_id, tmp2);
		return -1;
	}
	*tmp1 = '\0';
	cdr->split = atoi(tmp2);
	tmp2 = ++tmp1;

	tmp1 = strchr(tmp2, MED_SEP);
	if(tmp1 == NULL)
	{
		syslog(LOG_WARNING, "Call-Id '%s' has no separated destination external subscriber id, '%s'", cdr->call_id, tmp2);
		return -1;
	}
	*tmp1 = '\0';
	g_strlcpy(cdr->destination_ext_subscriber_id, tmp2, sizeof(cdr->destination_ext_subscriber_id));
	tmp2 = ++tmp1;

	tmp1 = strchr(tmp2, MED_SEP);
	if(tmp1 == NULL)
	{
		syslog(LOG_WARNING, "Call-Id '%s' has no separated destination external contract id, '%s'", cdr->call_id, tmp2);
		return -1;
	}
	*tmp1 = '\0';
	g_strlcpy(cdr->destination_ext_contract_id, tmp2, sizeof(cdr->destination_ext_contract_id));
	tmp2 = ++tmp1;

	tmp1 = strchr(tmp2, MED_SEP);
	if(tmp1 == NULL)
	{
		syslog(LOG_WARNING, "Call-Id '%s' has no separated destination account id, '%s'", cdr->call_id, tmp2);
		return -1;
	}
	*tmp1 = '\0';
	cdr->destination_account_id = atoll(tmp2);
	tmp2 = ++tmp1;

	tmp1 = strchr(tmp2, MED_SEP);
	if(tmp1 == NULL)
	{
		syslog(LOG_WARNING, "Call-Id '%s' has no separated dialed digits", cdr->call_id);
		return -1;
	}
	*tmp1 = '\0';
	g_strlcpy(cdr->destination_dialed, tmp2, sizeof(cdr->destination_dialed));
	tmp2 = ++tmp1;
	
	tmp1 = strchr(tmp2, MED_SEP);
	if(tmp1 == NULL)
	{
		syslog(LOG_WARNING, "Call-Id '%s' has no separated destination user id", cdr->call_id);
		return -1;
	}
	*tmp1 = '\0';
	g_strlcpy(cdr->destination_user_id, tmp2, sizeof(cdr->destination_user_id));
	tmp2 = ++tmp1;
	
	tmp1 = strchr(tmp2, MED_SEP);
	if(tmp1 == NULL)
	{
		syslog(LOG_WARNING, "Call-Id '%s' has no separated destination user", cdr->call_id);
		return -1;
	}
	*tmp1 = '\0';
	g_strlcpy(cdr->destination_user, tmp2, sizeof(cdr->destination_user));
	tmp2 = ++tmp1;

	tmp1 = strchr(tmp2, MED_SEP);
	if(tmp1 == NULL)
	{
		syslog(LOG_WARNING, "Call-Id '%s' has no separated destination domain", cdr->call_id);
		return -1;
	}
	*tmp1 = '\0';
	g_strlcpy(cdr->destination_domain, tmp2, sizeof(cdr->destination_domain));
	tmp2 = ++tmp1;

	tmp1 = strchr(tmp2, MED_SEP);
	if(tmp1 == NULL)
	{
		syslog(LOG_WARNING, "Call-Id '%s' has no separated incoming destination user", cdr->call_id);
		return -1;
	}
	*tmp1 = '\0';
	g_strlcpy(cdr->destination_user_in, tmp2, sizeof(cdr->destination_user_in));
	tmp2 = ++tmp1;

	tmp1 = strchr(tmp2, MED_SEP);
	if(tmp1 == NULL)
	{
		syslog(LOG_WARNING, "Call-Id '%s' has no separated incoming destination domain", cdr->call_id);
		return -1;
	}
	*tmp1 = '\0';
	g_strlcpy(cdr->destination_domain_in, tmp2, sizeof(cdr->destination_domain_in));
	tmp2 = ++tmp1;

	if(len < tmp2 - dstleg + 1)
		return 0;
	cdr->destination_lcr_id = atoll(tmp2);

	return 0;
}


int cdr_create_cdrs(med_entry_t *records, u_int64_t count, 
		cdr_entry_t **cdrs, u_int64_t *cdr_count, u_int8_t *trash)
{
	u_int64_t i = 0, cdr_index = 0;
	u_int32_t invites = 0;
	size_t cdr_size;

	char *endtime = NULL;
	double unix_endtime = 0, tmp_unix_endtime = 0;
	char *call_status;

	*cdr_count = 0;
	

	/* get end time from BYE's timestamp */
	for(i = 0; i < count; ++i)
	{
		med_entry_t *e = &(records[i]);
		if(e->valid && e->method == MED_INVITE)
		{
			++invites;
		}
		else if(e->method == MED_BYE && endtime == NULL)
		{
			endtime = e->timestamp;
			unix_endtime = e->unix_timestamp;
		}

		if (check_shutdown())
			return -1;
	}

	if(invites == 0)
	{
		syslog(LOG_CRIT, "No valid INVITEs for creating a cdr, internal error, callid='%s'", 
				records[0].callid);
		return -1;
	}

	/* each INVITE maps to a CDR */
	cdr_size = sizeof(cdr_entry_t) * invites;
	*cdrs = (cdr_entry_t*)malloc(cdr_size);
	if(*cdrs == NULL)
	{
		syslog(LOG_ERR, "Error allocating memory for cdrs: %s", strerror(errno));
		return -1;
	}
	memset(*cdrs, 0, cdr_size);

	for(i = 0; i < count; ++i)
	{
		med_entry_t *e = NULL;
		cdr_entry_t *cdr = NULL;


		cdr = &(*cdrs)[cdr_index];
		e = &(records[i]);

		call_status = cdr_map_status(e->sip_code);
		if(e->valid && e->method == MED_INVITE && call_status != NULL)
		{
			++cdr_index;

			if(strncmp("200", e->sip_code, 3))
			{
				/* missed calls have duration of 0 */
				tmp_unix_endtime = e->unix_timestamp;
			}
			else
			{
				tmp_unix_endtime = unix_endtime;
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

			if(cdr_parse_srcleg(e->src_leg, cdr) < 0)
			{
				*trash = 1;
				return 0;
			}
			
			if(cdr_parse_dstleg(e->dst_leg, cdr) < 0)
			{
				*trash = 1;
				return 0;
			}
			
			if(cdr_fill_record(cdr) != 0)
			{
				// TODO: error handling
			}
		}

		if (check_shutdown())
			return -1;
	}

	*cdr_count = cdr_index;

	/*syslog(LOG_DEBUG, "Created %llu CDRs:", *cdr_count);*/

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

