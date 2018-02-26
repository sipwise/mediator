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



#include "mediator.h"
#include "config.h"
#include "daemonizer.h"
#include "medmysql.h"
#include "medredis.h"
#include "cdr.h"

sig_atomic_t mediator_shutdown = 0;
int mediator_lockfd = -1;
uint64_t mediator_count = 0;

GHashTable *med_peer_ip_table = NULL;
GHashTable *med_peer_host_table = NULL;
GHashTable *med_peer_id_table = NULL;
GHashTable *med_uuid_table = NULL;
GHashTable *med_call_stat_info_table = NULL;

/**********************************************************************/
static int mediator_load_maps()
{
	med_peer_ip_table = g_hash_table_new_full(g_str_hash, g_str_equal, free, free);
	med_peer_host_table = g_hash_table_new_full(g_str_hash, g_str_equal, free, free);
	med_peer_id_table = g_hash_table_new_full(g_str_hash, g_str_equal, free, free);
	med_uuid_table = g_hash_table_new_full(g_str_hash, g_str_equal, free, free);

	if(medmysql_load_maps(med_peer_ip_table, med_peer_host_table, med_peer_id_table))
		return -1;
	if(medmysql_load_uuids(med_uuid_table))
		return -1;
	if (medmysql_load_db_ids())
		return -1;

	return 0;
}

/**********************************************************************/
static void mediator_print_mapentry(gpointer key, gpointer value, gpointer d __attribute__((unused)))
{
	syslog(LOG_DEBUG, "\t'%s' -> %s", (char*)key, (char*)value);
}

/**********************************************************************/
static void mediator_destroy_maps()
{
	if(med_peer_ip_table)
		g_hash_table_destroy(med_peer_ip_table);
	if(med_peer_host_table)
		g_hash_table_destroy(med_peer_host_table);
	if(med_peer_id_table)
		g_hash_table_destroy(med_peer_id_table);
	if(med_uuid_table)
		g_hash_table_destroy(med_uuid_table);
	if(med_call_stat_info_table)
		g_hash_table_destroy(med_call_stat_info_table);

	med_peer_ip_table = NULL;
	med_peer_host_table = NULL;
	med_peer_id_table = NULL;
	med_uuid_table = NULL;
	med_call_stat_info_table = NULL;
}

/**********************************************************************/
static void mediator_print_maps()
{
	syslog(LOG_DEBUG, "Peer IP map:");
	g_hash_table_foreach(med_peer_ip_table, mediator_print_mapentry, NULL);
	syslog(LOG_DEBUG, "Peer host map:");
	g_hash_table_foreach(med_peer_host_table, mediator_print_mapentry, NULL);
	syslog(LOG_DEBUG, "Peer ID map:");
	g_hash_table_foreach(med_peer_id_table, mediator_print_mapentry, NULL);
	syslog(LOG_DEBUG, "UUID map:");
	g_hash_table_foreach(med_uuid_table, mediator_print_mapentry, NULL);
}

/**********************************************************************/
static void mediator_unlock()
{
	syslog(LOG_DEBUG, "Unlocking mediator.");

	if(mediator_lockfd != -1)
	{
		flock(mediator_lockfd, LOCK_UN);
	}
}

/**********************************************************************/
static void mediator_exit()
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
static int mediator_lock()
{
	struct stat sb;

	mediator_lockfd = open(MEDIATOR_LOCK_FILE, O_CREAT|O_RDWR, S_IRUSR|S_IWUSR);
	if(mediator_lockfd == -1)
	{
		syslog(LOG_CRIT, "Error creating lock file: %s", strerror(errno));
		return -1;
	}
	if(flock(mediator_lockfd, LOCK_EX|LOCK_NB) == -1)
	{
		syslog(LOG_CRIT, "Error locking lock file: %s", strerror(errno));
		return -1;
	}
	if (fstat(mediator_lockfd, &sb)) {
		syslog(LOG_CRIT, "Error getting file stats for lock file: %m");
		return -1;
	}
	if (sb.st_size) {
		syslog(LOG_CRIT, "Non-empty lock file '%s' detected, refusing to start. Examine its contents to learn about the cause, and then delete it to clear the error", MEDIATOR_LOCK_FILE);
		return -1;
	}

	return 0;
}

#ifdef WITH_TIME_CALC
/**********************************************************************/
static uint64_t mediator_calc_runtime(struct timeval *tv_start, struct timeval *tv_stop)
{
	return ((uint64_t)((tv_stop->tv_sec * 1000000 + tv_stop->tv_usec) -
			   (tv_start->tv_sec * 1000000 + tv_start->tv_usec)) / 1000);
}
#endif


/**********************************************************************/
int main(int argc, char **argv)
{
	med_callid_t *mysql_callids;
	med_callid_t *redis_callids;
	med_entry_t *records;
	uint64_t mysql_id_count, redis_id_count, rec_count, i;
	uint64_t cdr_count, last_count;
	int maprefresh;
	struct medmysql_batches batches;

#ifdef WITH_TIME_CALC
	struct timeval tv_start, tv_stop;
	uint64_t runtime;
#endif	

	openlog(MEDIATOR_SYSLOG_NAME, LOG_PID|LOG_NDELAY, LOG_DAEMON);
	atexit(mediator_exit);

	signal(SIGCHLD, SIG_IGN);
	signal(SIGTERM, mediator_signal);
	signal(SIGINT, mediator_signal);

	if (config_parse(argc, argv) == -1)
	{
		return -1;
	}
	
	syslog(LOG_DEBUG, "Locking process.");
	if(mediator_lock() != 0)
	{
		return -1;
	}

	if(config_daemonize)
	{
		syslog(LOG_DEBUG, "Daemonizing process.");
		if(daemonize() != 0)
		{
			return -1;
		}
	}

	syslog(LOG_DEBUG, "Writing pid file.");
	if(write_pid(config_pid_path) != 0)
	{
		return -1;
	}

	if (config_maintenance) {
		syslog(LOG_INFO, "Maintenance mode active, going to sleep");
		while (!mediator_shutdown)
			sleep(1);
		exit(0);
	}
	
	syslog(LOG_INFO, "ACC acc database host='%s', port='%d', user='%s', name='%s'",
			config_med_host, config_med_port, config_med_user, config_med_db);
	syslog(LOG_INFO, "CDR acc database host='%s', port='%d', user='%s', name='%s'",
			config_cdr_host, config_cdr_port, config_cdr_user, config_cdr_db);
	syslog(LOG_INFO, "PROV database host='%s', port='%d', user='%s', name='%s'",
			config_prov_host, config_prov_port, config_prov_user, config_prov_db);
	syslog(LOG_INFO, "STATS database host='%s', port='%d', user='%s', name='%s'",
			config_stats_host, config_stats_port, config_stats_user, config_stats_db);
	syslog(LOG_INFO, "REDIS database host='%s', port='%d', pass='%s', id='%d'",
			config_redis_host, config_redis_port,
			config_redis_pass ? config_redis_pass : "<none>",
			config_redis_db);
	
	syslog(LOG_DEBUG, "Setting up mysql connections.");
	if(medmysql_init() != 0)
	{
		return -1;
	}
	syslog(LOG_DEBUG, "Setting up redis connections.");
	if(medredis_init() != 0)
	{
		return -1;
	}

	syslog(LOG_INFO, "Up and running, daemonized=%d, pid-path='%s', interval=%d",
			config_daemonize, config_pid_path, config_interval);

	maprefresh = 0;
	while(!mediator_shutdown)
	{
		if(maprefresh == 0)
		{
			mediator_destroy_maps();
			if(mediator_load_maps() != 0)
			{
				break;
			}
			maprefresh = 10;
		}
		--maprefresh;

		if (0)
			mediator_print_maps();

		mysql_id_count = redis_id_count = rec_count = cdr_count = 0;
		last_count = mediator_count;

		mysql_callids = medmysql_fetch_callids(&mysql_id_count);
		if(!mysql_callids && mysql_id_count) {
			/* error */
			break;
		}

		redis_callids = medredis_fetch_callids(&redis_id_count);
		if (!redis_callids && redis_id_count) {
			/* error */
			break;
		}

		if (!mysql_id_count && !redis_id_count) {
			goto idle;
		}

		if (medmysql_batch_start(&batches))
			break;


		//////////////// mysql handling //////////////////

		syslog(LOG_DEBUG, "Processing %"PRIu64" mysql accounting record group(s).", mysql_id_count);
		for(i = 0; i < mysql_id_count && !mediator_shutdown; ++i)
		{
#ifdef WITH_TIME_CALC
			gettimeofday(&tv_start, NULL);
#endif

			if(medmysql_fetch_records(&(mysql_callids[i]), &records, &rec_count) != 0)
				goto out;

			if(cdr_process_records(records, rec_count, &cdr_count, &batches) != 0)
				goto out;

			if(rec_count > 0)
			{
				free(records);
			}

			mediator_count += cdr_count;

#ifdef WITH_TIME_CALC
			gettimeofday(&tv_stop, NULL);
			runtime = mediator_calc_runtime(&tv_start, &tv_stop);
			syslog(LOG_DEBUG, "Runtime for mysql record group was %"PRIu64" ms.", runtime);
#endif
		}
		if (mysql_id_count) {
			free(mysql_callids);
		}


		//////////////// redis handling //////////////////

		syslog(LOG_DEBUG, "Processing %"PRIu64" redis accounting record group(s).", redis_id_count);
		for(i = 0; i < redis_id_count && !mediator_shutdown; ++i)
		{
#ifdef WITH_TIME_CALC
			gettimeofday(&tv_start, NULL);
#endif

			if(medredis_fetch_records(&(redis_callids[i]), &records, &rec_count) != 0)
				goto out;

			syslog(LOG_DEBUG, "process cdr with cid '%s' and %"PRIu64" records\n", redis_callids[i].value, rec_count);

			if (rec_count) {
				if(cdr_process_records(records, rec_count, &cdr_count, &batches) != 0)
					goto out;
				free(records);

				mediator_count += cdr_count;
			}

#ifdef WITH_TIME_CALC
			gettimeofday(&tv_stop, NULL);
			runtime = mediator_calc_runtime(&tv_start, &tv_stop);
			syslog(LOG_DEBUG, "Runtime for redis record group was %"PRIu64" ms.", runtime);
#endif
		}

		//////////////// end //////////////////

		if (medmysql_batch_end(&batches))
			break;

idle:
		if(mediator_count > last_count)
		{
			syslog(LOG_DEBUG, "Overall %"PRIu64" CDRs created so far.", mediator_count);
			sleep(3);
		}
		else
		{
			/* sleep if no cdrs have been created */
			sleep(config_interval);
		}
	}

out:
	mediator_destroy_maps();
	syslog(LOG_INFO, "Shutting down.");

	medmysql_cleanup();
	medredis_cleanup();

	syslog(LOG_INFO, "Successfully shut down.");
	return 0;
}


void critical(const char *msg) {
	write(mediator_lockfd, msg, strlen(msg));
	write(mediator_lockfd, "\n", 1);
}
