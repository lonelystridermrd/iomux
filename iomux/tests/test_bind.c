#define _GNU_SOURCE

#include "iomux.h"

#include <pthread.h>
#include <stdio.h>
#include <sys/epoll.h>
#include <unistd.h>

static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_t g_first;
static int g_seen;
static int g_mismatch;

/* Force every fd onto worker 1 so binding is observable. */
static uint32_t hash_to_one(int fd, void *arg)
{
	(void)fd;
	(void)arg;
	return 1u;
}

static void on_read(int fd, uint32_t events, void *arg)
{
	char buf[64];

	(void)events;
	(void)arg;
	(void)read(fd, buf, sizeof(buf));

	pthread_mutex_lock(&g_mu);
	if (g_seen == 0)
		g_first = pthread_self();
	else if (!pthread_equal(g_first, pthread_self()))
		g_mismatch++;
	g_seen++;
	pthread_mutex_unlock(&g_mu);
}

static void *reactor(void *arg)
{
	(void)iomux_run((struct iomux_poller *)arg);
	return NULL;
}

int main(void)
{
	struct iomux_cfg cfg;
	struct iomux_bind bind;
	struct iomux_poller *p;
	pthread_t th;
	int fds[2];
	int i;

	iomux_cfg_default(&cfg);
	cfg.nworkers = 4;

	p = iomux_new(&cfg);
	if (p == NULL) {
		fprintf(stderr, "test_bind: iomux_new failed\n");
		return 1;
	}
	if (pipe(fds) != 0)
		return 1;
	if (pthread_create(&th, NULL, reactor, p) != 0)
		return 1;

	bind.hash = hash_to_one;
	bind.arg = NULL;
	if (iomux_add(p, fds[0], EPOLLIN, on_read, NULL, &bind) != 0)
		return 1;

	for (i = 0; i < 30; i++) {
		if (write(fds[1], "x", 1) != 1)
			return 1;
		usleep(2000);
	}
	usleep(200000);

	iomux_stop(p);
	pthread_join(th, NULL);
	iomux_free(p);

	pthread_mutex_lock(&g_mu);
	if (g_seen == 0 || g_mismatch != 0) {
		fprintf(stderr, "test_bind: seen=%d mismatch=%d\n",
			g_seen, g_mismatch);
		pthread_mutex_unlock(&g_mu);
		return 1;
	}
	pthread_mutex_unlock(&g_mu);

	close(fds[0]);
	close(fds[1]);
	printf("test_bind: ok (seen=%d)\n", g_seen);
	return 0;
}
