#ifndef _CDR_H
#define _CDR_H

#include "mediator.h"

#define MSG_INVITE "INVITE"
#define MSG_BYE    "BYE"

#define CDR_STATUS_OK       "ok"
#define CDR_STATUS_BUSY     "busy"
#define CDR_STATUS_NA       "noanswer"
#define CDR_STATUS_CANCEL   "cancel"
#define CDR_STATUS_OFFLINE  "offline"
#define CDR_STATUS_TIMEOUT  "timeout"
#define CDR_STATUS_UNKNOWN  "other"

typedef struct {
	char call_id[128];

	char source_user_id[37];
	char source_ext_subscriber_id[256];
	char source_ext_contract_id[256];
	char source_provider_id[256];
	u_int64_t source_account_id;
	char source_user[256];
	char source_domain[256];
	char source_cli[65];
	char source_ip[65];
	u_int8_t source_clir;
	char source_gpp[10][255];
	char source_lnp_prefix[256];
	char source_user_out[256];
	char source_lnp_type[256];

	char destination_user_id[37];
	char destination_provider_id[256];
	char destination_ext_subscriber_id[256];
	char destination_ext_contract_id[256];
	u_int64_t destination_account_id;
	char destination_user[256];
	char destination_domain[256];
	char destination_user_in[256];
	char destination_domain_in[256];
	char destination_dialed[256];
	u_int64_t destination_lcr_id;
	char destination_gpp[10][255];
	char destination_lnp_prefix[256];
	char destination_user_out[256];
	char destination_lnp_type[256];

	char call_type[8];
	char call_status[16];
	char call_code[4];

	char peer_auth_user[256];
	char peer_auth_realm[256];

	double init_time;
	double start_time;
	double duration;

	u_int32_t source_carrier_cost;
	u_int32_t source_reseller_cost;
	u_int32_t source_customer_cost;
	u_int32_t destination_carrier_cost;
	u_int32_t destination_reseller_cost;
	u_int32_t destination_customer_cost;

	u_int8_t split;

} cdr_entry_t;

int cdr_process_records(med_entry_t *records, u_int64_t count, u_int64_t *cdr_count);
void cdr_fix_accids(med_entry_t *records, u_int64_t count);
int cdr_fill_record(cdr_entry_t *cdr);
void cdr_set_provider(cdr_entry_t *cdr);


#endif /* _CDR_H */
