#include "mediator.h"

#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <time.h>
#include <sys/time.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <glib.h>
#include <sys/resource.h>



#include "config.h"
#include "daemonizer.h"
#include "medmysql.h"
#include "cdr.h"

sig_atomic_t mediator_shutdown = 0;
int mediator_lockfd = -1;
static u_int64_t mediator_count = 0;
static pthread_mutex_t mediator_count_lock = PTHREAD_MUTEX_INITIALIZER;

pthread_rwlock_t med_tables_lock = PTHREAD_RWLOCK_INITIALIZER;
struct med_tables med_tables;

static
pthread_t signal_thread,
	  map_reload_thread,
	  callid_fetch_thread,
	  callid_work_threads[4];

static GQueue callid_process_queue = G_QUEUE_INIT;
static pthread_cond_t callid_process_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t callid_process_lock = PTHREAD_MUTEX_INITIALIZER;
static int process_threads_busy;
static pthread_cond_t process_idle_cond = PTHREAD_COND_INITIALIZER;


/**********************************************************************/
static int __mediator_load_maps(struct med_tables *t)
{
	t->peer_ip = g_hash_table_new_full(g_str_hash, g_str_equal, free, free);
	t->peer_host = g_hash_table_new_full(g_str_hash, g_str_equal, free, free);
	t->peer_id = g_hash_table_new_full(g_str_hash, g_str_equal, free, free);
	t->uuid = g_hash_table_new_full(g_str_hash, g_str_equal, free, free);

	if(medmysql_load_maps(t))
		return -1;
	if(medmysql_load_uuids(t->uuid))
		return -1;

	return 0;
}

/**********************************************************************/
static void mediator_print_mapentry(gpointer key, gpointer value, gpointer d)
{
	syslog(LOG_DEBUG, "\t'%s' -> %s", (char*)key, (char*)value);
}

/**********************************************************************/
static void __mediator_destroy_maps(struct med_tables *t)
{
	if(t->peer_ip)
		g_hash_table_destroy(t->peer_ip);
	if(t->peer_host)
		g_hash_table_destroy(t->peer_host);
	if(t->peer_id)
		g_hash_table_destroy(t->peer_id);
	if(t->uuid)
		g_hash_table_destroy(t->uuid);

	t->peer_ip = NULL;
	t->peer_host = NULL;
	t->peer_id = NULL;
	t->uuid = NULL;
}

static int mediator_reload_maps() {
	struct med_tables tmp_new, tmp_old;

	if (__mediator_load_maps(&tmp_new))
		return -1;

	pthread_rwlock_wrlock(&med_tables_lock);
	tmp_old = med_tables;
	med_tables = tmp_new;
	pthread_rwlock_unlock(&med_tables_lock);
	__mediator_destroy_maps(&tmp_old);

	return 0;
}

/**********************************************************************/
static void mediator_print_maps()
{
	pthread_rwlock_rdlock(&med_tables_lock);
	syslog(LOG_DEBUG, "Peer IP map:");
	g_hash_table_foreach(med_tables.peer_ip, mediator_print_mapentry, NULL);
	syslog(LOG_DEBUG, "Peer host map:");
	g_hash_table_foreach(med_tables.peer_host, mediator_print_mapentry, NULL);
	syslog(LOG_DEBUG, "Peer ID map:");
	g_hash_table_foreach(med_tables.peer_id, mediator_print_mapentry, NULL);
	syslog(LOG_DEBUG, "UUID map:");
	g_hash_table_foreach(med_tables.uuid, mediator_print_mapentry, NULL);
	pthread_rwlock_unlock(&med_tables_lock);
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
static u_int64_t mediator_calc_runtime(struct timeval *tv_start, struct timeval *tv_stop)
{
	return ((u_int64_t)((tv_stop->tv_sec * 1000000 + tv_stop->tv_usec) -
			   (tv_start->tv_sec * 1000000 + tv_start->tv_usec)) / 1000);
}
#endif


/* thread func */
static void *signal_handler(void *p) {
	sigset_t ss;
	int ret, sig;

	sigemptyset(&ss);
	sigaddset(&ss, SIGINT);
	sigaddset(&ss, SIGTERM);

	while (!mediator_shutdown) {
		ret = sigwait(&ss, &sig);
		if (ret == EAGAIN || ret == EINTR)
			continue;
		if (ret)
			abort();

		mediator_signal(sig);
	}

	return NULL;
}


static void signals(void) {
	sigset_t ss;
	struct rlimit rlim;

	memset(&rlim, 0, sizeof(rlim));
	rlim.rlim_cur = rlim.rlim_max = RLIM_INFINITY;
	setrlimit(RLIMIT_CORE, &rlim);

	sigfillset(&ss);
	sigdelset(&ss, SIGABRT);
	sigdelset(&ss, SIGSEGV);
	sigdelset(&ss, SIGQUIT);
	sigprocmask(SIG_SETMASK, &ss, NULL);
	pthread_sigmask(SIG_SETMASK, &ss, NULL);

	if (pthread_create(&signal_thread, NULL, signal_handler, NULL))
		abort();
}



/* thread func */
static void *map_reloader(void *p) {
	if (medmysql_init())
		abort();

	while (!mediator_shutdown) {
		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
		usleep(30000000);
		pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
		if (mediator_reload_maps())
			abort();
		if (0)
			mediator_print_maps();
	}

	return NULL;
}



/* thread func */
static void *callid_fetcher(void *p) {
	GQueue callids;

	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);

	if (medmysql_init())
		abort();

	while (!mediator_shutdown) {
		g_queue_init(&callids);

		if (medmysql_fetch_callids(&callids))
			abort();

		if (!callids.length) {
			pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
			usleep(config_interval * 1000000);
			pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
			continue;
		}

		/* append our list to the global queue */
		pthread_mutex_lock(&callid_process_lock);
		if (!callid_process_queue.length)
			callid_process_queue = callids;
		else {
			callid_process_queue.tail->next = callids.head;
			callids.head->prev = callid_process_queue.tail;
			callid_process_queue.length += callids.length;
		}
		/* our queue is now invalid and will be cleared on next iteration */

		/* work to do! */

		/* this wakes up all worker threads. we then wait for them to finish
		 * before fetching the next batch. this is necessary so we don't keep
		 * polling the same callids. */
		pthread_cond_broadcast(&callid_process_cond);
		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
		pthread_cleanup_push((void *) pthread_mutex_unlock, &callid_process_lock);
		do
			pthread_cond_wait(&process_idle_cond, &callid_process_lock);
		while (process_threads_busy);
		pthread_cleanup_pop(1);
		pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);

		pthread_mutex_lock(&mediator_count_lock);
		syslog(LOG_DEBUG, "Overall %"PRIu64" CDRs created so far.", mediator_count);
		pthread_mutex_unlock(&mediator_count_lock);
	}

	medmysql_cleanup();
	return NULL;
}




/* thread func */
static void *callid_worker(void *p) {
	char *callid;
	struct medmysql_batches batches;
	med_entry_t *records;
	u_int64_t rec_count, cdr_count;
#ifdef WITH_TIME_CALC
	struct timeval tv_start, tv_stop;
	u_int64_t runtime;
#endif	

	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);

	if (medmysql_init())
		abort();

	if (medmysql_batch_start(&batches))
		abort();

	pthread_mutex_lock(&callid_process_lock);
	process_threads_busy++;
	pthread_mutex_unlock(&callid_process_lock);

	while (!mediator_shutdown) {
		pthread_mutex_lock(&callid_process_lock);

		/* if nothing to do right now, flush SQL queues */
		if (!callid_process_queue.length) {
			pthread_mutex_unlock(&callid_process_lock);
			if (medmysql_batch_end(&batches))
				abort();
			if (medmysql_batch_start(&batches))
				abort();
			pthread_mutex_lock(&callid_process_lock);
		}

		/* if still nothing to do, wait for more */
		if (!callid_process_queue.length) {
#ifdef WITH_TIME_CALC
			gettimeofday(&tv_stop, NULL);
			runtime = mediator_calc_runtime(&tv_start, &tv_stop);
			syslog(LOG_DEBUG, "Runtime for record group was %"PRIu64" ms.", runtime);
#endif

			process_threads_busy--;
			pthread_cond_signal(&process_idle_cond);
			pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
			pthread_cleanup_push((void *) pthread_mutex_unlock, &callid_process_lock);
			pthread_cond_wait(&callid_process_cond, &callid_process_lock);
			pthread_cleanup_pop(0);
			pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
			process_threads_busy++;

#ifdef WITH_TIME_CALC
			gettimeofday(&tv_start, NULL);
#endif
		}

		callid = g_queue_pop_head(&callid_process_queue);
		pthread_mutex_unlock(&callid_process_lock);

		if (!callid)
			continue;

		/* we got one */
		records = NULL;
		rec_count = 0;
		cdr_count = 0;

		if(medmysql_fetch_records(callid, &records, &rec_count) != 0)
			abort();

		if(cdr_process_records(records, rec_count, &cdr_count, &batches) != 0)
			abort();

		/* stats and cleanup */
		pthread_mutex_lock(&mediator_count_lock);
		mediator_count += cdr_count;
		pthread_mutex_unlock(&mediator_count_lock);

		if (records)
			free(records);
		free(callid);
	}

	return NULL;
}


/**********************************************************************/
int main(int argc, char **argv)
{
	int i;

#if !GLIB_CHECK_VERSION(2,32,0)
	g_thread_init(NULL);
#endif

	openlog(MEDIATOR_SYSLOG_NAME, LOG_PID|LOG_NDELAY, LOG_DAEMON);
	atexit(mediator_exit);

	if(config_parse_cmdopts(argc, argv) == -1)
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

	signals();
	
	syslog(LOG_INFO, "ACC acc database host='%s', port='%d', user='%s', name='%s'",
			config_med_host, config_med_port, config_med_user, config_med_db);
	syslog(LOG_INFO, "CDR acc database host='%s', port='%d', user='%s', name='%s'",
			config_cdr_host, config_cdr_port, config_cdr_user, config_cdr_db);
	
	syslog(LOG_DEBUG, "Setting up mysql connections.");
	if(medmysql_init() != 0)
	{
		return -1;
	}

	syslog(LOG_INFO, "Up and running, daemonized=%d, pid-path='%s', interval=%d",
			config_daemonize, config_pid_path, config_interval);

	/* initialize */
	if (mediator_reload_maps())
		abort();

	/* start chugging away */
	if (pthread_create(&map_reload_thread, NULL, map_reloader, NULL))
		abort();
	if (pthread_create(&callid_fetch_thread, NULL, callid_fetcher, NULL))
		abort();
	for (i = 0; i < G_N_ELEMENTS(callid_work_threads); i++)
		if (pthread_create(&callid_work_threads[i], NULL, callid_worker, NULL))
			abort();

	/* the signal thread terminates when we should shut down */
	pthread_join(signal_thread, NULL);
	syslog(LOG_INFO, "Shutting down.");

	pthread_cancel(map_reload_thread);
	pthread_join(map_reload_thread, NULL);
	pthread_cancel(callid_fetch_thread);
	pthread_join(callid_fetch_thread, NULL);
	for (i = 0; i < G_N_ELEMENTS(callid_work_threads); i++) {
		pthread_cancel(callid_work_threads[i]);
		pthread_join(callid_work_threads[i], NULL);
	}

	__mediator_destroy_maps(&med_tables);

	medmysql_cleanup();

	syslog(LOG_INFO, "Successfully shut down.");
	return 0;
}


void critical(const char *msg) {
	write(mediator_lockfd, msg, strlen(msg));
	write(mediator_lockfd, "\n", 1);
}
