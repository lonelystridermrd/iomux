#define _GNU_SOURCE

#include "iomux.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define CONNS 8000
#define MSGS 500000
#define RSS_LIMIT_KB 65536L
#define WAIT_MS 180000

static int *g_peer;
static atomic_long g_recv;

static void on_read(int fd, uint32_t events, void *arg)
{
	char buf[8];
	ssize_t n;

	(void)events;
	(void)arg;
	n = read(fd, buf, sizeof(buf));
	if (n > 0)
		atomic_fetch_add_explicit(&g_recv, (long)n, memory_order_relaxed);
}

static void *reactor(void *arg)
{
	(void)iomux_run((struct iomux_poller *)arg);
	return NULL;
}

static long rss_kb(void)
{
	FILE *f = fopen("/proc/self/status", "r");
	char line[256];
	long value = 0;

	if (f == NULL)
		return 0;
	while (fgets(line, sizeof(line), f) != NULL) {
		if (strncmp(line, "VmRSS:", 6) == 0) {
			(void)sscanf(line + 6, "%ld", &value);
			break;
		}
	}
	(void)fclose(f);
	return value;
}

static uint64_t now_ms(void)
{
	struct timespec ts;

	(void)clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000ull +
	       (uint64_t)ts.tv_nsec / 1000000ull;
}

int main(void)
{
	struct iomux_cfg cfg;
	struct iomux_poller *p;
	pthread_t th;
	int (*pair)[2];
	int i;
	long received;
	long rss;
	uint64_t t0;
	uint64_t elapsed;

	pair = malloc(sizeof(*pair) * CONNS);
	g_peer = malloc(sizeof(*g_peer) * CONNS);
	if (pair == NULL || g_peer == NULL)
		return 1;

	iomux_cfg_default(&cfg);
	cfg.nworkers = 4;
	cfg.max_watchers = CONNS * 2u;
	cfg.queue_depth = 4096;
	cfg.timer_capacity = 16;

	p = iomux_new(&cfg);
	if (p == NULL) {
		fprintf(stderr, "test_scale: iomux_new failed\n");
		return 1;
	}

	for (i = 0; i < CONNS; i++) {
		if (socketpair(AF_UNIX, SOCK_STREAM, 0, pair[i]) != 0)
			return 1;
		g_peer[i] = pair[i][1];
		if (iomux_add(p, pair[i][0], EPOLLIN, on_read, NULL, NULL) != 0)
			return 1;
	}

	if (pthread_create(&th, NULL, reactor, p) != 0)
		return 1;

	t0 = now_ms();
	for (i = 0; i < MSGS; i++) {
		if (write(g_peer[i % CONNS], "x", 1) != 1)
			return 1;
	}

	while (atomic_load(&g_recv) < MSGS &&
	       now_ms() - t0 < WAIT_MS)
		usleep(10000);

	elapsed = now_ms() - t0;
	received = atomic_load(&g_recv);
	rss = rss_kb();

	iomux_stop(p);
	pthread_join(th, NULL);
	(void)iomux_free(p);

	if (received != MSGS) {
		fprintf(stderr, "test_scale: received=%ld expect=%d\n",
			received, MSGS);
		return 1;
	}
	if (rss > RSS_LIMIT_KB) {
		fprintf(stderr, "test_scale: rss=%ldKB over limit %ldKB\n",
			rss, RSS_LIMIT_KB);
		return 1;
	}

	printf("test_scale: ok (conns=%d msgs=%d rss_kb=%ld elapsed_ms=%llu rate=%.0f/s)\n",
	       CONNS, MSGS, rss, (unsigned long long)elapsed,
	       (double)MSGS * 1000.0 / (double)elapsed);
	printf("SCALE connections=%d messages=%d\n", CONNS, MSGS);
	return 0;
}
