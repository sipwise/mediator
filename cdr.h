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
#define CDR_STATUS_FAILED   "failed"
#define CDR_STATUS_UNKNOWN  "other"

struct medmysql_batches;

typedef struct {
    int filled;
    double avg_score;
    int avg_packetloss;
    int avg_jitter;
    int avg_rtt;
} mos_data_t;

#define F(f, l) char f[l];
#define FA(f, a, l) char f[a][l];

typedef struct {
#include "cdr_field_names.inc"

    uint64_t source_account_id;
    uint8_t source_clir;
    uint64_t source_lcr_id;

    uint64_t destination_account_id;
    uint64_t destination_lcr_id;

    double init_time;
    double start_time;
    double duration;

    uint32_t source_carrier_cost;
    uint32_t source_reseller_cost;
    uint32_t source_customer_cost;
    uint32_t destination_carrier_cost;
    uint32_t destination_reseller_cost;
    uint32_t destination_customer_cost;

    uint8_t split;

    mos_data_t mos;

    unsigned int intermediate:1;
} cdr_entry_t;

#undef F
#undef FA

int cdr_process_records(med_entry_t *records, uint64_t count, uint64_t *cdr_count, struct medmysql_batches *,
        int do_intermediate);
void cdr_fix_accids(med_entry_t *records, uint64_t count);
int cdr_fill_record(cdr_entry_t *cdr);
void cdr_set_provider(cdr_entry_t *cdr);


#endif /* _CDR_H */
