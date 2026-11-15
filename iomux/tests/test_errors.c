#define _GNU_SOURCE

#include "iomux.h"

#include <pthread.h>
#include <stdio.h>
#include <sys/epoll.h>
#include <unistd.h>

static void on_read(int fd, uint32_t events, void *arg)
{
	(void)fd;
	(void)events;
	(void)arg;
}

static void *reactor(void *arg)
{
	(void)iomux_run((struct iomux_poller *)arg);
	return NULL;
}

int main(void)
{
	struct iomux_cfg cfg;
	struct iomux_poller *p;
	struct iomux_poller *bad;
	pthread_t th;
	int fds[2];
	int err = 0;
	uint64_t id;

	if (iomux_add(NULL, 0, EPOLLIN, on_read, NULL, NULL) != -EINVAL)
		return 1;

	iomux_cfg_default(&cfg);
	cfg.nworkers = 0;			/* invalid */
	bad = iomux_new(&cfg);
	if (bad != NULL)
		return 1;

	iomux_cfg_default(&cfg);
	p = iomux_new(&cfg);
	if (p == NULL)
		return 1;
	if (pipe(fds) != 0)
		return 1;

	if (iomux_add(p, -1, EPOLLIN, on_read, NULL, NULL) != -EINVAL)
		return 1;
	if (iomux_add(p, fds[0], 0, on_read, NULL, NULL) != -EINVAL)
		return 1;
	if (iomux_add(p, fds[0], EPOLLIN, NULL, NULL, NULL) != -EINVAL)
		return 1;

	if (iomux_add(p, fds[0], EPOLLIN, on_read, NULL, NULL) != 0)
		return 1;
	if (iomux_add(p, fds[0], EPOLLIN, on_read, NULL, NULL) != -EEXIST)
		return 1;
	if (iomux_del(p, fds[0] + 1000) != -ENOENT)
		return 1;

	id = iomux_timer_add(p, 1, 0, NULL, NULL, &err);
	if (id != 0 || err != -EINVAL)
		return 1;

	if (pthread_create(&th, NULL, reactor, p) != 0)
		return 1;
	usleep(50000);
	iomux_stop(p);
	pthread_join(th, NULL);

	/* After run() returned, control calls must fail fast. */
	if (iomux_del(p, fds[0]) != -ESHUTDOWN)
		return 1;

	iomux_free(p);
	close(fds[0]);
	close(fds[1]);
	printf("test_errors: ok\n");
	return 0;
}
