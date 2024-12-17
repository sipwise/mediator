#ifndef _CDR_H
#define _CDR_H

#include "mediator.h"
#include <stdbool.h>

#define MSG_INVITE "INVITE"
#define MSG_BYE    "BYE"
#define MSG_REFER  "REFER"

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

#define F(f, x) GString *f;
#define FA(f, a, x) GString *f[a];

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

int cdr_process_records(GQueue *records, uint64_t *cdr_count, struct medmysql_batches *,
        int do_intermediate);
void cdr_parse_entry(med_entry_t *);
void cdr_truncate_call_id_suffix(char *);
bool cdr_verify_fields(const cdr_entry_t *);
bool cdr_write_error_record(const char *fn, const char *s, size_t);


#endif /* _CDR_H */
