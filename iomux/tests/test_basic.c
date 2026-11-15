#define _GNU_SOURCE

#include "iomux.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <sys/epoll.h>
#include <unistd.h>

static int g_pipe[2];
static atomic_int g_hits;

static void on_read(int fd, uint32_t events, void *arg)
{
	char buf[64];
	ssize_t n;

	(void)events;
	(void)arg;
	n = read(fd, buf, sizeof(buf));
	if (n > 0)
		atomic_fetch_add_explicit(&g_hits, 1, memory_order_relaxed);
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
	pthread_t th;
	int i;

	iomux_cfg_default(&cfg);
	cfg.nworkers = 2;

	p = iomux_new(&cfg);
	if (p == NULL) {
		fprintf(stderr, "test_basic: iomux_new failed\n");
		return 1;
	}
	if (pipe(g_pipe) != 0)
		return 1;
	if (pthread_create(&th, NULL, reactor, p) != 0)
		return 1;
	if (iomux_add(p, g_pipe[0], EPOLLIN, on_read, NULL, NULL) != 0)
		return 1;

	for (i = 0; i < 100; i++) {
		if (write(g_pipe[1], "x", 1) != 1)
			return 1;
		usleep(1000);
	}
	usleep(200000);

	iomux_stop(p);
	pthread_join(th, NULL);

	if (atomic_load(&g_hits) != 100) {
		fprintf(stderr, "test_basic: hits=%d, expect 100\n",
			atomic_load(&g_hits));
		return 1;
	}

	iomux_free(p);
	close(g_pipe[0]);
	close(g_pipe[1]);
	printf("test_basic: ok (hits=%d)\n", atomic_load(&g_hits));
	return 0;
}
