#ifndef _DAEMONIZER_H
#define _DAEMONIZER_H

#include "mediator.h"

int daemonize(void);
int write_pid(const char *pidfile);

#endif /* _DAEMONIZER_H */
