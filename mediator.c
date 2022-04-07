#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <time.h>
#include <sys/time.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <systemd/sd-daemon.h>



#include "mediator.h"
#include "config.h"
#include "daemonizer.h"
#include "medmysql.h"
#include "medredis.h"
#include "cdr.h"
#include "records.h"

sig_atomic_t mediator_shutdown = 0;
int mediator_lockfd = -1;
uint64_t mediator_count = 0;
static time_t next_intermediate_run = 0;

GHashTable *med_peer_ip_table = NULL;
GHashTable *med_peer_host_table = NULL;
GHashTable *med_peer_id_table = NULL;
GHashTable *med_uuid_cache = NULL;
GHashTable *med_call_stat_info_table = NULL;
GHashTable *med_cdr_tag_table = NULL;


static void med_free_cache_entry(void *p)
{
    med_cache_entry_t *e = p;
    g_free(e->str_value);
    g_slice_free1(sizeof(*e), e);
}
void med_entry_free(void *p) {
    med_entry_t *e = p;
    g_free(e->callid);
    g_free(e->dst_leg);
    g_free(e->src_leg);
    g_free(e->acc_ref);
    json_object_put(e->dst_leg_json);
    json_object_put(e->src_leg_json);
    g_slice_free1(sizeof(*e), e);
}

/**********************************************************************/
static void mediator_create_caches(void)
{
    med_uuid_cache = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, med_free_cache_entry);
}

static int mediator_load_maps(void)
{
    med_peer_ip_table = g_hash_table_new_full(g_str_hash, g_str_equal, free, free);
    med_peer_host_table = g_hash_table_new_full(g_str_hash, g_str_equal, free, free);
    med_peer_id_table = g_hash_table_new_full(g_str_hash, g_str_equal, free, free);
    med_cdr_tag_table = g_hash_table_new_full(g_str_hash, g_str_equal, free, NULL);

    if(medmysql_load_maps(med_peer_ip_table, med_peer_host_table, med_peer_id_table))
        return -1;
    if (medmysql_load_db_ids())
        return -1;
    if (medmysql_load_cdr_tag_ids(med_cdr_tag_table))
        return -1;

    return 0;
}

/**********************************************************************/
static void mediator_print_mapentry(gpointer key, gpointer value, gpointer d __attribute__((unused)))
{
    L_DEBUG("\t'%s' -> %s", (char*)key, (char*)value);
}

/**********************************************************************/
static void mediator_destroy_caches(void)
{
    g_hash_table_destroy(med_uuid_cache);
    med_uuid_cache = NULL;
}

static void mediator_cleanup_caches(void)
{
    medmysql_cache_cleanup(med_uuid_cache);
}

static void mediator_destroy_maps(void)
{
    if(med_peer_ip_table)
        g_hash_table_destroy(med_peer_ip_table);
    if(med_peer_host_table)
        g_hash_table_destroy(med_peer_host_table);
    if(med_peer_id_table)
        g_hash_table_destroy(med_peer_id_table);
    if(med_call_stat_info_table)
        g_hash_table_destroy(med_call_stat_info_table);
    if(med_cdr_tag_table)
        g_hash_table_destroy(med_cdr_tag_table);

    med_peer_ip_table = NULL;
    med_peer_host_table = NULL;
    med_peer_id_table = NULL;
    med_call_stat_info_table = NULL;
    med_cdr_tag_table = NULL;
}

/**********************************************************************/
static void mediator_print_maps(void)
{
    L_DEBUG("Peer IP map:");
    g_hash_table_foreach(med_peer_ip_table, mediator_print_mapentry, NULL);
    L_DEBUG("Peer host map:");
    g_hash_table_foreach(med_peer_host_table, mediator_print_mapentry, NULL);
    L_DEBUG("Peer ID map:");
    g_hash_table_foreach(med_peer_id_table, mediator_print_mapentry, NULL);
    L_DEBUG("TAGS map:");
    g_hash_table_foreach(med_cdr_tag_table, mediator_print_mapentry, NULL);
}

/**********************************************************************/
static void mediator_unlock(void)
{
    L_DEBUG("Unlocking mediator.");

    if(mediator_lockfd != -1)
    {
        flock(mediator_lockfd, LOCK_UN);
    }
}

/**********************************************************************/
static void mediator_exit(void)
{
    mediator_unlock();
    config_cleanup();
    closelog();
}

/**********************************************************************/
static void mediator_signal(int signal)
{
    if(signal == SIGTERM || signal == SIGINT)
        mediator_shutdown = 1;
}

/**********************************************************************/
static int mediator_lock(void)
{
    struct stat sb;

    mediator_lockfd = open(MEDIATOR_LOCK_FILE, O_CREAT|O_RDWR, S_IRUSR|S_IWUSR);
    if(mediator_lockfd == -1)
    {
        L_CRITICAL("Error creating lock file: %s", strerror(errno));
        return -1;
    }
    if(flock(mediator_lockfd, LOCK_EX|LOCK_NB) == -1)
    {
        L_CRITICAL("Error locking lock file: %s", strerror(errno));
        return -1;
    }
    if (fstat(mediator_lockfd, &sb)) {
        L_CRITICAL("Error getting file stats for lock file: %m");
        return -1;
    }
    if (sb.st_size) {
        L_CRITICAL("Non-empty lock file '%s' detected, refusing to start. Examine its contents to learn about the cause, and then delete it to clear the error", MEDIATOR_LOCK_FILE);
        return -1;
    }

    return 0;
}

/**********************************************************************/
static uint64_t mediator_calc_runtime(struct timeval *tv_start, struct timeval *tv_stop)
{
    return ((uint64_t)((tv_stop->tv_sec * 1000000 + tv_stop->tv_usec) -
               (tv_start->tv_sec * 1000000 + tv_start->tv_usec)));
}

/**********************************************************************/
int main(int argc, char **argv)
{
    GQueue mysql_callids = G_QUEUE_INIT;
    GQueue redis_callids = G_QUEUE_INIT;
    GQueue acc_records = G_QUEUE_INIT;
    uint64_t cdr_count, last_count;
    int maprefresh;
    struct medmysql_batches *batches;
    struct timeval loop_tv_start, loop_tv_stop;
    uint64_t loop_runtime;
#ifdef WITH_TIME_CALC
    struct timeval tv_start, tv_stop;
    uint64_t runtime;
#endif    
    gboolean success;

    openlog(MEDIATOR_SYSLOG_NAME, LOG_PID|LOG_NDELAY, LOG_DAEMON);
    atexit(mediator_exit);

    signal(SIGCHLD, SIG_IGN);
    signal(SIGTERM, mediator_signal);
    signal(SIGINT, mediator_signal);

    if (config_parse(argc, argv) == -1)
    {
        return -1;
    }

    L_NOTICE("Starting mediator\n");
    
    L_DEBUG("Locking process.");
    if(mediator_lock() != 0)
    {
        return -1;
    }

    if(config_daemonize)
    {
        L_DEBUG("Daemonizing process.");
        if(daemonize() != 0)
        {
            return -1;
        }
    }

    L_DEBUG("Writing pid file.");
    if(write_pid(config_pid_path) != 0)
    {
        return -1;
    }

    if (config_maintenance) {
        L_INFO("Maintenance mode active, going to sleep");
        sd_notify(0, "READY=1\n");
        while (!mediator_shutdown)
            sleep(1);
        exit(0);
    }
    
    L_INFO("ACC acc database host='%s', port='%d', user='%s', name='%s'",
            config_med_host, config_med_port, config_med_user, config_med_db);
    L_INFO("CDR acc database host='%s', port='%d', user='%s', name='%s'",
            config_cdr_host, config_cdr_port, config_cdr_user, config_cdr_db);
    L_INFO("PROV database host='%s', port='%d', user='%s', name='%s'",
            config_prov_host, config_prov_port, config_prov_user, config_prov_db);
    L_INFO("STATS database host='%s', port='%d', user='%s', name='%s'",
            config_stats_host, config_stats_port, config_stats_user, config_stats_db);
    L_INFO("REDIS database host='%s', port='%d', pass='%s', id='%d'",
            config_redis_host, config_redis_port,
            config_redis_pass ? config_redis_pass : "<none>",
            config_redis_db);
    
    L_DEBUG("Setting up mysql connections.");
    if(medmysql_init() != 0)
    {
        return -1;
    }
    L_DEBUG("Setting up redis connections.");
    if(medredis_init() != 0)
    {
        return -1;
    }

    L_NOTICE("Up and running, daemonized=%d, pid-path='%s', interval=%d",
            config_daemonize, config_pid_path, config_interval);
    sd_notify(0, "READY=1\n");

    maprefresh = 0;
    batches = malloc(sizeof(*batches));
    if (!batches) {
        L_ERROR("Out of memory allocating batches");
        return -1;
    }

    mediator_create_caches();

    while(!mediator_shutdown)
    {
        L_DEBUG("Starting mediation loop\n");
        gettimeofday(&loop_tv_start, NULL);
        if(maprefresh == 0)
        {
            mediator_destroy_maps();
            if(mediator_load_maps() != 0)
            {
                break;
            }
            mediator_cleanup_caches();
            maprefresh = 10;
        }
        --maprefresh;

        if (0)
            mediator_print_maps();

        // process intermediate CDRs this round?
        int do_intermediate = 0;
        if (config_intermediate_interval > 0) {
            if (loop_tv_start.tv_sec >= next_intermediate_run) {
                L_DEBUG("Processing intermediate CDRs in this iteration\n");
                do_intermediate = 1;
                if (next_intermediate_run == 0)
                    next_intermediate_run = loop_tv_start.tv_sec;
                next_intermediate_run += config_intermediate_interval;
            }
        }

        g_queue_clear_full(&mysql_callids, g_free);
        g_queue_clear_full(&redis_callids, g_free);
        g_queue_clear_full(&acc_records, med_entry_free);
        cdr_count = 0;
        last_count = mediator_count;

        success = medmysql_fetch_callids(&mysql_callids);
        if(!success) {
            L_ERROR("Failed to fetch callids from MySQL\n");
            break;
        }

        success = medredis_fetch_callids(&redis_callids);
        if (!success) {
            L_ERROR("Failed to fetch callids from Redis\n");
            break;
        }

        if (!mysql_callids.length && !redis_callids.length) {
            L_DEBUG("No callids found, going idle\n");
            goto idle;
        }

        if (medmysql_batch_start(batches)) {
            L_ERROR("Failed to start MySQL batches\n");
            break;
        }


        //////////////// mysql handling //////////////////

        L_DEBUG("Processing %u mysql accounting record group(s).", mysql_callids.length);
        for(GList *l = mysql_callids.head; l && !mediator_shutdown; l = l->next)
        {
            char *mysql_callid = l->data;
#ifdef WITH_TIME_CALC
            gettimeofday(&tv_start, NULL);
#endif

            int ret = medmysql_fetch_records(mysql_callid, &acc_records, 1);
            if (ret < 0)
                goto out;
            int must_sort = (ret > 0);

            ret = medredis_fetch_records(mysql_callid, &acc_records);
            if (ret > 0)
                must_sort = 1;

            if (must_sort)
            {
                // only re-sort if records from Redis were added, as MySQL already does the sorting
                records_sort(&acc_records);
            }

            int are_records_complete = records_complete(&acc_records);

            if (!are_records_complete && !do_intermediate)
            {
                L_DEBUG("Found incomplete call with cid '%s', skipping...\n", mysql_callid);
                g_queue_clear_full(&acc_records, med_entry_free);
                continue;
            }

            if(cdr_process_records(&acc_records, &cdr_count, batches, do_intermediate) != 0)
                goto out;

            g_queue_clear_full(&acc_records, med_entry_free);

            mediator_count += cdr_count;

#ifdef WITH_TIME_CALC
            gettimeofday(&tv_stop, NULL);
            runtime = mediator_calc_runtime(&tv_start, &tv_stop);
            L_INFO("Runtime for mysql record group was %"PRIu64" us.", runtime);
            L_INFO("CDR creation rate for mysql record group was %f CDR/sec", (double)mysql_callids.length/runtime * 1000000);
#endif
        }


        //////////////// redis handling //////////////////

        L_DEBUG("Processing %u redis accounting record group(s).", redis_callids.length);
        for(GList *l = redis_callids.head; l && !mediator_shutdown; l = l->next)
        {
            char *redis_callid = l->data;
#ifdef WITH_TIME_CALC
            gettimeofday(&tv_start, NULL);
#endif

            if(medredis_fetch_records(redis_callid, &acc_records) < 0)
                goto out;

            medmysql_fetch_records(redis_callid, &acc_records, 0);

            // always sort records from Redis, regardless of whether records from MySQL were merged
            records_sort(&acc_records);

            int are_records_complete = records_complete(&acc_records);

            if (!are_records_complete && !do_intermediate)
            {
                L_DEBUG("Found incomplete call with cid '%s', skipping...\n", redis_callid);
                g_queue_clear_full(&acc_records, med_entry_free);
                continue;
            }

            L_DEBUG("process cdr with cid '%s' and %u records\n", redis_callid, acc_records.length);

            if (acc_records.length) {
                if(cdr_process_records(&acc_records, &cdr_count, batches, do_intermediate) != 0) {
                    g_queue_clear_full(&acc_records, med_entry_free);
                    goto out;
                }
                g_queue_clear_full(&acc_records, med_entry_free);

                mediator_count += cdr_count;
            }

#ifdef WITH_TIME_CALC
            gettimeofday(&tv_stop, NULL);
            runtime = mediator_calc_runtime(&tv_start, &tv_stop);
            L_INFO("Runtime for redis record group was %"PRIu64" us.", runtime);
            L_INFO("CDR creation rate for redis record group was %f CDR/sec", (double)redis_callids.length/runtime * 1000000);
#endif
        }

        //////////////// end //////////////////

        if (medmysql_batch_end(batches))
            break;

        gettimeofday(&loop_tv_stop, NULL);
        loop_runtime = mediator_calc_runtime(&loop_tv_start, &loop_tv_stop);
        L_INFO("Runtime for loop processing %u callids and generating %"PRIu64" CDRs was %"PRIu64" us.",
            mysql_callids.length + redis_callids.length, mediator_count - last_count, loop_runtime);
        L_INFO("Total CDR creation rate %f CDR/sec", (double)(mediator_count - last_count)/loop_runtime * 1000000);

idle:
        if(mediator_count > last_count)
        {
            L_DEBUG("Overall %"PRIu64" CDRs created so far.", mediator_count);
            sleep(3);
        }
        else
        {
            /* sleep if no cdrs have been created */
            sleep(config_interval);
        }
    }

out:
    L_INFO("Shutting down.");
    sd_notify(0, "STOPPING=1\n");

    g_queue_clear_full(&acc_records, med_entry_free);
    g_queue_clear_full(&mysql_callids, g_free);
    g_queue_clear_full(&redis_callids, g_free);
    mediator_destroy_maps();
    mediator_destroy_caches();
    medmysql_cleanup();
    medredis_cleanup();
    free(batches);

    L_INFO("Successfully shut down.");
    return 0;
}


void critical(const char *msg) {
    write(mediator_lockfd, msg, strlen(msg));
    write(mediator_lockfd, "\n", 1);
}
