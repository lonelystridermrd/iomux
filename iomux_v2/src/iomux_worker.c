/*
 * iomux_worker.c - worker threads, per-worker SPSC queues, completion path.
 */

#define _GNU_SOURCE

#include "iomux_internal.h"

#include <errno.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/resource.h>
#include <unistd.h>

/**
 * @brief Best-effort eventfd signal; EAGAIN means a wakeup is already pending.
 *
 * @param fd registered descriptor.
 */
static void wake_fd(int fd)
{
	uint64_t one = 1u;
	ssize_t rc = write(fd, &one, sizeof(one));

	(void)rc;
}

/**
 * @brief SPSC emptiness check used before deciding to wake a worker.
 *
 * @param r ring buffer.
 * @return 0 on success, negative errno on failure.
 */
static int ring_is_empty(const struct iomux_ring *r)
{
	return atomic_load_explicit(&r->tail, memory_order_relaxed) ==
	       atomic_load_explicit(&r->head, memory_order_acquire);
}

/**
 * @brief Return a finished task to the reactor; spin only while the ring is full.
 *
 * @param w watcher slot.
 * @param t task descriptor.
 */
static void completion_push(struct iomux_worker *w, const struct iomux_task *t)
{
	int was_empty = ring_is_empty(&w->out);

	while (iomux_ring_push(&w->out, t) != 0) {
		if (atomic_load(&w->stop))
			return;
		sched_yield();
	}

	if (was_empty) {
		/* Best effort: the reactor drains the ring anyway. */
		wake_fd(w->p->comp_fd);
	}
}

/**
 * @brief Run queued callbacks, then report completion.
 *
 * @param w watcher slot.
 */
static void worker_drain(struct iomux_worker *w)
{
	struct iomux_task t;

	while (iomux_ring_pop(&w->in, &t) == 0) {
		if (t.type == IOMUX_TASK_FD) {
			struct iomux_watcher *wt = t.obj;

			wt->cb(t.fd, t.events, wt->arg);
		} else {
			struct iomux_timer *tm = t.obj;

			tm->cb(tm->arg);
		}
		w->processed++;
		completion_push(w, &t);
	}
}

/**
 * @brief Block on the wake eventfd, drain the queue, exit on stop.
 *
 * @param arg opaque user pointer.
 */
static void *worker_main(void *arg)
{
	struct iomux_worker *w = arg;
	uint64_t counter;

	while (!atomic_load(&w->stop)) {
		ssize_t n = read(w->wake_fd, &counter, sizeof(counter));

		if (n < 0) {
			if (errno == EINTR)
				continue;
			break;
		}
		if (atomic_load(&w->stop))
			break;
		worker_drain(w);
	}
	return NULL;
}

/**
 * @brief Create worker threads with an explicit stack size from RLIMIT_STACK.
 *
 * @param p poller instance.
 * @return 0 on success, negative errno on failure.
 */
int iomux_worker_start(struct iomux_poller *p)
{
	pthread_attr_t attr;
	struct rlimit rl;
	size_t stack = 8u * 1024u * 1024u;
	uint32_t i;
	int have_attr = 0;
	int rc = 0;

	/* Explicit stack size: never rely on the libc default. */
	if (getrlimit(RLIMIT_STACK, &rl) == 0 &&
	    rl.rlim_cur != RLIM_INFINITY && rl.rlim_cur >= 256u * 1024u)
		stack = (size_t)rl.rlim_cur;

	if (pthread_attr_init(&attr) == 0) {
		have_attr = 1;
		(void)pthread_attr_setstacksize(&attr, stack);
	}

	p->workers = calloc(p->nworkers, sizeof(*p->workers));
	if (p->workers == NULL) {
		rc = -ENOMEM;
		goto out;
	}

	for (i = 0u; i < p->nworkers; i++) {
		struct iomux_worker *w = &p->workers[i];
		char name[32];

		w->p = p;
		w->index = i;
		w->wake_fd = -1;
		w->started = 0;
		atomic_init(&w->stop, 0);

		rc = iomux_ring_init(&w->in, p->cfg.queue_depth);
		if (rc != 0)
			goto out;
		rc = iomux_ring_init(&w->out, p->cfg.queue_depth);
		if (rc != 0)
			goto out;

		w->wake_fd = eventfd(0, EFD_CLOEXEC);
		if (w->wake_fd < 0) {
			rc = -errno;
			goto out;
		}

		rc = pthread_create(&w->tid, have_attr ? &attr : NULL,
				   worker_main, w);
		if (rc != 0) {
			rc = -rc;
			goto out;
		}
		w->started = 1;

		(void)snprintf(name, sizeof(name), "iomux-w%u", i);
		(void)pthread_setname_np(w->tid, name);
	}

out:
	if (have_attr)
		(void)pthread_attr_destroy(&attr);
	return rc;
}

/**
 * @brief Set the stop flag and wake every worker.
 *
 * @param p poller instance.
 */
void iomux_worker_stop(struct iomux_poller *p)
{
	uint32_t i;

	if (p->workers == NULL)
		return;

	for (i = 0u; i < p->nworkers; i++) {
		struct iomux_worker *w = &p->workers[i];

		atomic_store(&w->stop, 1);
		if (w->wake_fd >= 0)
			wake_fd(w->wake_fd);
	}
}

/**
 * @brief Join started workers and release their rings.
 *
 * @param p poller instance.
 */
void iomux_worker_join(struct iomux_poller *p)
{
	uint32_t i;

	if (p->workers == NULL)
		return;

	for (i = 0u; i < p->nworkers; i++) {
		struct iomux_worker *w = &p->workers[i];

		if (w->started)
			(void)pthread_join(w->tid, NULL);
		if (w->wake_fd >= 0)
			(void)close(w->wake_fd);
		iomux_ring_destroy(&w->in);
		iomux_ring_destroy(&w->out);
	}
	free(p->workers);
	p->workers = NULL;
}
