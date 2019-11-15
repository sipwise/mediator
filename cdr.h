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

struct medmysql_batches;

typedef struct {
    int filled;
    double avg_score;
    int avg_packetloss;
    int avg_jitter;
    int avg_rtt;
} mos_data_t;

typedef struct {
    char call_id[128];

    char source_user_id[37];
    char source_ext_subscriber_id[256];
    char source_ext_contract_id[256];
    char source_provider_id[256];
    uint64_t source_account_id;
    char source_user[256];
    char source_domain[256];
    char source_cli[65];
    char source_ip[65];
    uint8_t source_clir;
    uint64_t source_lcr_id;
    char source_gpp[10][255];
    char source_lnp_prefix[256];
    char source_user_out[256];
    char source_lnp_type[256];

    char destination_user_id[37];
    char destination_provider_id[256];
    char destination_ext_subscriber_id[256];
    char destination_ext_contract_id[256];
    uint64_t destination_account_id;
    char destination_user[256];
    char destination_domain[256];
    char destination_user_in[256];
    char destination_domain_in[256];
    char destination_dialed[256];
    uint64_t destination_lcr_id;
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

    uint32_t source_carrier_cost;
    uint32_t source_reseller_cost;
    uint32_t source_customer_cost;
    uint32_t destination_carrier_cost;
    uint32_t destination_reseller_cost;
    uint32_t destination_customer_cost;

    char furnished_charging_info[256];
    char header_diversion[2048];
    char header_pai[2048];
    char header_u2u[256];

    char group[256];

    uint8_t split;

    mos_data_t mos;
} cdr_entry_t;

int cdr_process_records(med_entry_t *records, uint64_t count, uint64_t *cdr_count, struct medmysql_batches *);
void cdr_fix_accids(med_entry_t *records, uint64_t count);
int cdr_fill_record(cdr_entry_t *cdr);
void cdr_set_provider(cdr_entry_t *cdr);


#endif /* _CDR_H */
