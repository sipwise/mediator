#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "daemonizer.h"

int daemonize()
{
	pid_t pid = fork();
	if(pid < 0)
	{
		return -1;
	}
	else if(pid > 0)
	{
		_exit(0);
	}
	else if(pid == 0)
	{
		int fds;
		setsid();
		for(fds = getdtablesize(); fds >= 0; --fds) 
		{
			if(fds != mediator_lockfd)
				close(fds);
		}
		fds = open("/dev/null", O_RDWR); /* stdin */
		if(fds < 0)
		{
			syslog(LOG_CRIT, "Error opening /dev/null: %s", strerror(errno));
			return -1;
		}
		if(dup(fds) < 0) {}; /* stdout */
		if(dup(fds) < 0) {}; /* stderr */
		umask(027);
		if(chdir("/") < 0) {};
	}

	return 0;
}

int write_pid(const char *pidfile)
{
	FILE *pfile = fopen(pidfile, "w");
	if(pfile == NULL)
	{
		syslog(LOG_CRIT, "Error opening pid file '%s': %s", pidfile, strerror(errno));
		return -1;
	}
	fprintf(pfile, "%d", getpid());
	fclose(pfile);
	return 0;
}
