#define _GNU_SOURCE

#include "iomux.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <unistd.h>

static atomic_int g_ticks;

static void on_tick(void *arg)
{
	(void)arg;
	atomic_fetch_add_explicit(&g_ticks, 1, memory_order_relaxed);
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
	uint64_t id;
	int err = 0;
	int n1;
	int n2;

	iomux_cfg_default(&cfg);
	cfg.nworkers = 1;

	p = iomux_new(&cfg);
	if (p == NULL) {
		fprintf(stderr, "test_timer: iomux_new failed\n");
		return 1;
	}
	if (pthread_create(&th, NULL, reactor, p) != 0)
		return 1;

	id = iomux_timer_add(p, 10, 10, on_tick, NULL, &err);
	if (id == 0 || err != 0) {
		fprintf(stderr, "test_timer: add failed err=%d\n", err);
		return 1;
	}

	usleep(150000);
	n1 = atomic_load(&g_ticks);
	if (n1 < 3) {
		fprintf(stderr, "test_timer: too few ticks=%d\n", n1);
		return 1;
	}

	if (iomux_timer_cancel(p, id) != 0) {
		fprintf(stderr, "test_timer: cancel failed\n");
		return 1;
	}
	usleep(50000);
	n2 = atomic_load(&g_ticks);
	if (n2 > n1 + 1) {
		fprintf(stderr, "test_timer: ticks kept growing %d -> %d\n", n1, n2);
		return 1;
	}

	iomux_stop(p);
	pthread_join(th, NULL);
	iomux_free(p);

	printf("test_timer: ok (ticks=%d)\n", n1);
	return 0;
}
