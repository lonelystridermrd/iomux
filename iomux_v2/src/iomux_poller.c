/*
 * iomux_poller.c - poller lifecycle, epoll reactor, fd table, command queue.
 *
 * Thread ownership:
 *  - epoll fd, watcher table, timer heap, pending list: reactor thread only.
 *  - command queue: any thread may enqueue; the reactor executes entries.
 *  - Before iomux_run() is called, commands execute inline on the caller.
 */

#define _GNU_SOURCE

#include "iomux_internal.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define IOMUX_FREE_WAIT_MS	5000u
#define IOMUX_FREE_WAIT_STEP_US	50000u

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
 * @brief Consume the eventfd counter so the next epoll_wait can block again.
 *
 * @param fd registered descriptor.
 */
static void drain_fd(int fd)
{
	uint64_t value;
	ssize_t rc = read(fd, &value, sizeof(value));

	(void)rc;
}

/**
 * @brief Capture (st_dev, st_ino) so fd-number reuse can be detected.
 *
 * @param fd registered descriptor.
 * @param dev out: st_dev of the descriptor.
 * @param ino out: st_ino of the descriptor.
 * @return 0 on success, negative errno on failure.
 */
static int fd_identity(int fd, dev_t *dev, ino_t *ino)
{
	struct stat st;

	if (fstat(fd, &st) != 0)
		return -1;
	*dev = st.st_dev;
	*ino = st.st_ino;
	return 0;
}

/**
 * @brief SPSC emptiness check: producer tail equals consumer head.
 *
 * @param r ring buffer.
 * @return 0 on success, negative errno on failure.
 */
static int ring_empty(const struct iomux_ring *r)
{
	return atomic_load_explicit(&r->tail, memory_order_relaxed) ==
	       atomic_load_explicit(&r->head, memory_order_acquire);
}

/**
 * @brief Monotonic milliseconds; never wall clock, so NTP jumps cannot skew timers.
 *
 * @return Timer id, or 0 on failure (see err).
 */
uint64_t iomux_now_ms(void)
{
	struct timespec ts;

	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
		return 0u;
	return (uint64_t)ts.tv_sec * 1000ull +
	       (uint64_t)ts.tv_nsec / 1000000ull;
}

/**
 * @brief Forward to cfg.log; silent when no sink is configured.
 *
 * @param p poller instance.
 * @param level input parameter.
 * @param fmt input parameter.
 * @param ... input parameter.
 */
void iomux_log(struct iomux_poller *p, int level, const char *fmt, ...)
{
	va_list ap;

	if (p == NULL || p->cfg.log == NULL || fmt == NULL)
		return;
	va_start(ap, fmt);
	p->cfg.log(level, fmt, ap);
	va_end(ap);
}

/**
 * @brief Round up to a power of two for the bucket array.
 *
 * @param v input parameter.
 */
static uint32_t next_pow2(uint32_t v)
{
	uint32_t r = 1u;

	while (r < v)
		r <<= 1u;
	return r;
}

/**
 * @brief Knuth multiplicative hash over the fd number.
 *
 * @param fd registered descriptor.
 */
static uint32_t fd_hash(int fd)
{
	uint32_t x = (uint32_t)fd;

	x *= 2654435761u;
	x ^= x >> 15;
	return x;
}

/**
 * @brief Fill a config with safe defaults; every field may be overridden.
 *
 * @param cfg configuration; NULL selects defaults.
 */
void iomux_cfg_default(struct iomux_cfg *cfg)
{
	if (cfg == NULL)
		return;
	memset(cfg, 0, sizeof(*cfg));
	cfg->nworkers = 2u;
	cfg->max_events = 512u;
	cfg->queue_depth = 1024u;
	cfg->max_watchers = 4096u;
	cfg->timer_capacity = 1024u;
	cfg->log = NULL;
}

/**
 * @brief Reject configs that would break invariants (worker range, power-of-two queue).
 *
 * @param c command entry.
 * @return 0 on success, negative errno on failure.
 */
static int cfg_valid(const struct iomux_cfg *c)
{
	if (c->nworkers < 1u || c->nworkers > IOMUX_MAX_WORKERS)
		return 0;
	if (c->max_events < 1u || c->max_events > IOMUX_MAX_EVENTS)
		return 0;
	if (c->queue_depth < IOMUX_MIN_QUEUE ||
	    (c->queue_depth & (c->queue_depth - 1u)) != 0u)
		return 0;
	if (c->max_watchers < 16u)
		return 0;
	if (c->timer_capacity < 16u)
		return 0;
	return 1;
}

/**
 * @brief Internal helper.
 *
 * @param p poller instance.
 * @param fd registered descriptor.
 */

static struct iomux_watcher *watcher_find(struct iomux_poller *p, int fd)
{
	uint32_t bucket = fd_hash(fd) & p->bucket_mask;
	struct iomux_watcher *w;

	for (w = p->buckets[bucket]; w != NULL; w = w->hnext) {
		if (w->fd == fd)
			return w;
	}
	return NULL;
}

/**
 * @brief Insert into the fd hash chain (reactor thread only).
 *
 * @param p poller instance.
 * @param w watcher slot.
 */
static void watcher_link(struct iomux_poller *p, struct iomux_watcher *w)
{
	uint32_t bucket = fd_hash(w->fd) & p->bucket_mask;

	w->hnext = p->buckets[bucket];
	p->buckets[bucket] = w;
}

/**
 * @brief Remove from the fd hash chain; safe when not linked.
 *
 * @param p poller instance.
 * @param w watcher slot.
 */
static void watcher_unlink(struct iomux_poller *p, struct iomux_watcher *w)
{
	uint32_t bucket = fd_hash(w->fd) & p->bucket_mask;
	struct iomux_watcher **pp = &p->buckets[bucket];

	while (*pp != NULL) {
		if (*pp == w) {
			*pp = w->hnext;
			w->hnext = NULL;
			return;
		}
		pp = &(*pp)->hnext;
	}
}

/**
 * @brief Take a zeroed watcher slot from the preallocated pool.
 *
 * @param p poller instance.
 */
static struct iomux_watcher *watcher_alloc(struct iomux_poller *p)
{
	struct iomux_watcher *w;

	if (p->nfree == 0u)
		return NULL;
	w = p->wheel[--p->nfree];
	memset(w, 0, sizeof(*w));
	w->state = IOMUX_IDLE;
	w->fd = -1;
	return w;
}

/**
 * @brief Return a watcher slot; caller must have unlinked it.
 *
 * @param p poller instance.
 * @param w watcher slot.
 */
static void watcher_free(struct iomux_poller *p, struct iomux_watcher *w)
{
	watcher_unlink(p, w);
	w->state = IOMUX_DEAD;
	w->fd = -1;
	w->cb = NULL;
	w->arg = NULL;
	if (p->nfree < p->max_watchers)
		p->wheel[p->nfree++] = w;
}

/**
 * @brief Preallocate watcher pool and buckets; no malloc after init.
 *
 * @param p poller instance.
 * @return 0 on success, negative errno on failure.
 */
static int alloc_watchers(struct iomux_poller *p)
{
	uint32_t i;
	uint32_t nbuckets;

	p->max_watchers = p->cfg.max_watchers;
	p->watchers = calloc(p->max_watchers, sizeof(*p->watchers));
	p->wheel = calloc(p->max_watchers, sizeof(*p->wheel));
	if (p->watchers == NULL || p->wheel == NULL)
		return -ENOMEM;

	nbuckets = next_pow2(p->max_watchers * 2u);
	p->buckets = calloc(nbuckets, sizeof(*p->buckets));
	if (p->buckets == NULL)
		return -ENOMEM;

	p->nbuckets = nbuckets;
	p->bucket_mask = nbuckets - 1u;
	for (i = 0u; i < p->max_watchers; i++)
		p->wheel[i] = &p->watchers[i];
	p->nfree = p->max_watchers;
	return 0;
}

/**
 * @brief Internal helper.
 *
 * @param p poller instance.
 * @param t task descriptor.
 * @return 0 on success, negative errno on failure.
 */

int iomux_dispatch(struct iomux_poller *p, struct iomux_task *t)
{
	struct iomux_worker *w;
	int was_empty;

	if (t->worker >= p->nworkers)
		return -EINVAL;

	w = &p->workers[t->worker];
	was_empty = ring_empty(&w->in);
	if (iomux_ring_push(&w->in, t) != 0)
		return -EAGAIN;
	if (was_empty)
		wake_fd(w->wake_fd);

	p->stats.dispatched++;
	return 0;
}

/**
 * @brief Queue a watcher whose target worker queue was full.
 *
 * @param p poller instance.
 * @param w watcher slot.
 */
static void pending_add(struct iomux_poller *p, struct iomux_watcher *w)
{
	w->pend_next = NULL;
	if (p->pend_tail != NULL)
		p->pend_tail->pend_next = w;
	else
		p->pend_head = w;
	p->pend_tail = w;
}

/**
 * @brief Retry pending dispatches once per reactor iteration.
 *
 * @param p poller instance.
 */
static void pending_flush(struct iomux_poller *p)
{
	struct iomux_watcher *w = p->pend_head;

	p->pend_head = NULL;
	p->pend_tail = NULL;

	while (w != NULL) {
		struct iomux_watcher *next = w->pend_next;
		struct iomux_task t;

		w->pend_next = NULL;
		if (w->state != IOMUX_INFLIGHT) {
			w = next;
			continue;
		}

		t.type = IOMUX_TASK_FD;
		t.worker = w->worker;
		t.fd = w->fd;
		t.events = w->events;
		t.obj = w;
		if (iomux_dispatch(p, &t) != 0)
			pending_add(p, w);
		w = next;
	}
}

/**
 * @brief Hash affinity when registered, round-robin otherwise.
 *
 * @param p poller instance.
 * @param w watcher slot.
 */
static uint32_t choose_worker(struct iomux_poller *p, struct iomux_watcher *w)
{
	if (w->hash != NULL)
		return w->hash(w->fd, w->hash_arg) % p->nworkers;
	return atomic_fetch_add(&p->rr, 1u) % p->nworkers;
}

/**
 * @brief Re-arm EPOLLONESHOT after a callback; drop the watcher if fd identity changed.
 *
 * @param p poller instance.
 * @param w watcher slot.
 */
static void watcher_rearm(struct iomux_poller *p, struct iomux_watcher *w)
{
	struct epoll_event ev;

	if (w->has_ident) {
		dev_t dev;
		ino_t ino;

		if (fd_identity(w->fd, &dev, &ino) != 0 ||
		    dev != w->fdev || ino != w->fino) {
			p->stats.errors++;
			iomux_log(p, IOMUX_LOG_WARN,
				  "fd %d identity changed, dropping watcher",
				  w->fd);
			watcher_free(p, w);
			return;
		}
	}

	memset(&ev, 0, sizeof(ev));
	ev.events = w->events | EPOLLONESHOT;
	ev.data.ptr = w;

	if (epoll_ctl(p->epfd, EPOLL_CTL_MOD, w->fd, &ev) != 0) {
		p->stats.errors++;
		iomux_log(p, IOMUX_LOG_WARN, "rearm fd %d failed (%s), dropping",
			  w->fd, strerror(errno));
		watcher_free(p, w);
		return;
	}
	p->stats.rearmed++;
}

/**
 * @brief Completion handler: rearm a live watcher, free a dead one.
 *
 * @param p poller instance.
 * @param done input parameter.
 */
void iomux_reap(struct iomux_poller *p, struct iomux_task *done)
{
	if (done->type == IOMUX_TASK_FD) {
		struct iomux_watcher *w = done->obj;

		if (w == NULL)
			return;
		if (w->state == IOMUX_DEAD) {
			watcher_free(p, w);
			return;
		}
		w->state = IOMUX_IDLE;
		watcher_rearm(p, w);
		return;
	}
	iomux_timer_reap(p, done->obj);
}

/**
 * @brief Drain every worker completion ring; reactor thread only.
 *
 * @param p poller instance.
 */
static void drain_completions(struct iomux_poller *p)
{
	uint32_t i;

	for (i = 0u; i < p->nworkers; i++) {
		struct iomux_task t;

		while (iomux_ring_pop(&p->workers[i].out, &t) == 0)
			iomux_reap(p, &t);
	}
}

/**
 * @brief Mark INFLIGHT, pick a worker, enqueue; fall back to pending on a full queue.
 *
 * @param p poller instance.
 * @param ptr input parameter.
 * @param events EPOLL* interest mask.
 */
static void handle_event(struct iomux_poller *p, void *ptr, uint32_t events)
{
	struct iomux_watcher *w = ptr;
	struct iomux_task t;

	if (w == NULL || w->state != IOMUX_IDLE)
		return;

	w->state = IOMUX_INFLIGHT;
	w->worker = choose_worker(p, w);

	t.type = IOMUX_TASK_FD;
	t.worker = w->worker;
	t.fd = w->fd;
	t.events = events;
	t.obj = w;

	if (iomux_dispatch(p, &t) != 0) {
		p->stats.queue_full++;
		pending_add(p, w);
	}
}

/**
 * @brief Register fd: reject duplicates, replace a stale same-number watcher.
 *
 * @param p poller instance.
 * @param c command entry.
 * @return 0 on success, negative errno on failure.
 */

static int cmd_add(struct iomux_poller *p, struct iomux_cmd *c)
{
	struct iomux_watcher *w;
	struct iomux_watcher *old;
	struct epoll_event ev;
	dev_t dev = 0;
	ino_t ino = 0;
	int has_ident = 0;

	old = watcher_find(p, c->fd);
	if (old != NULL) {
		if (old->has_ident && fd_identity(c->fd, &dev, &ino) == 0 &&
		    (dev != old->fdev || ino != old->fino)) {
			/* Same fd number, different open file: drop the stale one. */
			watcher_free(p, old);
		} else {
			return -EEXIST;
		}
	}

	w = watcher_alloc(p);
	if (w == NULL)
		return -ENOSPC;

	if (fd_identity(c->fd, &dev, &ino) == 0)
		has_ident = 1;

	w->fd = c->fd;
	w->events = c->events;
	w->cb = c->cb;
	w->arg = c->arg;
	w->hash = c->bind.hash;
	w->hash_arg = c->bind.arg;
	w->has_ident = (uint8_t)has_ident;
	w->fdev = dev;
	w->fino = ino;

	memset(&ev, 0, sizeof(ev));
	ev.events = c->events | EPOLLONESHOT;
	ev.data.ptr = w;
	if (epoll_ctl(p->epfd, EPOLL_CTL_ADD, c->fd, &ev) != 0) {
		int err = errno;

		w->fd = -1;
		watcher_free(p, w);
		return -err;
	}

	watcher_link(p, w);
	return 0;
}

/**
 * @brief Change the interest mask; -EBUSY while a callback is in flight.
 *
 * @param p poller instance.
 * @param c command entry.
 * @return 0 on success, negative errno on failure.
 */
static int cmd_mod(struct iomux_poller *p, struct iomux_cmd *c)
{
	struct iomux_watcher *w = watcher_find(p, c->fd);
	struct epoll_event ev;

	if (w == NULL)
		return -ENOENT;
	if (w->state != IOMUX_IDLE)
		return -EBUSY;

	memset(&ev, 0, sizeof(ev));
	ev.events = c->events | EPOLLONESHOT;
	ev.data.ptr = w;
	if (epoll_ctl(p->epfd, EPOLL_CTL_MOD, c->fd, &ev) != 0)
		return -errno;

	w->events = c->events;
	return 0;
}

/**
 * @brief Unregister; defer the free to completion when INFLIGHT.
 *
 * @param p poller instance.
 * @param c command entry.
 * @return 0 on success, negative errno on failure.
 */
static int cmd_del(struct iomux_poller *p, struct iomux_cmd *c)
{
	struct iomux_watcher *w = watcher_find(p, c->fd);

	if (w == NULL)
		return -ENOENT;

	(void)epoll_ctl(p->epfd, EPOLL_CTL_DEL, c->fd, NULL);
	if (w->state == IOMUX_INFLIGHT)
		w->state = IOMUX_DEAD;
	else
		watcher_free(p, w);
	return 0;
}

/**
 * @brief Execute one queued command on the reactor thread.
 *
 * @param p poller instance.
 * @param c command entry.
 * @return 0 on success, negative errno on failure.
 */
static int cmd_exec(struct iomux_poller *p, struct iomux_cmd *c)
{
	switch (c->op) {
	case IOMUX_CMD_ADD:
		return cmd_add(p, c);
	case IOMUX_CMD_MOD:
		return cmd_mod(p, c);
	case IOMUX_CMD_DEL:
		return cmd_del(p, c);
	case IOMUX_CMD_TIMER_ADD: {
		uint64_t id = 0u;
		int rc = iomux_timer_push(p, c->delay_ms, c->interval_ms,
					  c->tcb, c->arg, &id);

		if (rc == 0)
			c->timer_id = id;
		return rc;
	}
	case IOMUX_CMD_TIMER_CANCEL:
		return iomux_timer_remove(p, c->timer_id);
	default:
		return -EINVAL;
	}
}

/**
 * @brief Enqueue a command and block until the reactor reports a real errno.
 *
 * @param p poller instance.
 * @param c command entry.
 * @return 0 on success, negative errno on failure.
 */
static int cmd_submit(struct iomux_poller *p, struct iomux_cmd *c)
{
	int res;

	c->done = 0;
	c->result = -EINPROGRESS;

	pthread_mutex_lock(&p->cmd_mu);
	while ((p->cmd_tail - p->cmd_head) >= IOMUX_CMD_CAP &&
	       !atomic_load(&p->stopping))
		pthread_cond_wait(&p->cmd_cv, &p->cmd_mu);

	if (atomic_load(&p->stopping)) {
		pthread_mutex_unlock(&p->cmd_mu);
		return -ESHUTDOWN;
	}

	p->cmds[p->cmd_tail % IOMUX_CMD_CAP] = c;
	p->cmd_tail++;
	pthread_mutex_unlock(&p->cmd_mu);

	wake_fd(p->stop_fd);

	pthread_mutex_lock(&p->cmd_mu);
	while (!c->done && !atomic_load(&p->stopping))
		pthread_cond_wait(&p->cmd_cv, &p->cmd_mu);
	res = c->done ? c->result : -ESHUTDOWN;
	pthread_mutex_unlock(&p->cmd_mu);
	return res;
}

/**
 * @brief Inline before run(), queued while running, -ESHUTDOWN after stop.
 *
 * @param p poller instance.
 * @param c command entry.
 * @return 0 on success, negative errno on failure.
 */
static int call_cmd(struct iomux_poller *p, struct iomux_cmd *c)
{
	if (atomic_load(&p->started) && !atomic_load(&p->running))
		return -ESHUTDOWN;
	if (!atomic_load(&p->running))
		return cmd_exec(p, c);
	return cmd_submit(p, c);
}

/**
 * @brief Pop commands one by one and signal each completion.
 *
 * @param p poller instance.
 */
static void drain_commands(struct iomux_poller *p)
{
	for (;;) {
		struct iomux_cmd *c;

		pthread_mutex_lock(&p->cmd_mu);
		if (p->cmd_head == p->cmd_tail) {
			pthread_mutex_unlock(&p->cmd_mu);
			break;
		}
		c = p->cmds[p->cmd_head % IOMUX_CMD_CAP];
		p->cmd_head++;
		pthread_mutex_unlock(&p->cmd_mu);

		c->result = cmd_exec(p, c);

		pthread_mutex_lock(&p->cmd_mu);
		c->done = 1;
		pthread_cond_broadcast(&p->cmd_cv);
		pthread_mutex_unlock(&p->cmd_mu);
	}
}

/**
 * @brief Reactor loop: wait, dispatch, expire timers, run commands, reap.
 *
 * @param p poller instance.
 * @return 0 on success, negative errno on failure.
 */

int iomux_run(struct iomux_poller *p)
{
	struct epoll_event *evs;
	uint32_t max_events;
	int ret = 0;

	if (p == NULL)
		return -EINVAL;
	if (atomic_exchange(&p->running, 1) == 1)
		return -EBUSY;

	atomic_store(&p->stopping, 0);
	atomic_store(&p->started, 1);
	p->reactor_tid = pthread_self();
	atomic_store(&p->reactor_tid_valid, 1);

	max_events = p->cfg.max_events;
	evs = calloc(max_events, sizeof(*evs));
	if (evs == NULL) {
		atomic_store(&p->running, 0);
		atomic_store(&p->reactor_tid_valid, 0);
		return -ENOMEM;
	}

	while (atomic_load(&p->running)) {
		uint64_t now = iomux_now_ms();
		int timeout = iomux_timer_next_ms(p, now);
		int n = epoll_wait(p->epfd, evs, (int)max_events, timeout);
		uint32_t i;

		if (n < 0) {
			if (errno == EINTR)
				continue;
			p->stats.errors++;
			iomux_log(p, IOMUX_LOG_ERROR, "epoll_wait: %s",
				  strerror(errno));
			ret = -errno;
			break;
		}

		p->stats.epoll_batches++;
		p->stats.epoll_events += (uint64_t)n;
		now = iomux_now_ms();

		for (i = 0u; i < (uint32_t)n; i++) {
			void *ptr = evs[i].data.ptr;

			if (ptr == &p->stop_tag) {
				drain_fd(p->stop_fd);
				continue;
			}
			if (ptr == &p->comp_tag) {
				drain_fd(p->comp_fd);
				drain_completions(p);
				continue;
			}
			handle_event(p, ptr, evs[i].events);
		}

		iomux_timer_fire(p, now);
		drain_commands(p);
		pending_flush(p);
		drain_completions(p);
	}

	free(evs);
	atomic_store(&p->running, 0);
	atomic_store(&p->reactor_tid_valid, 0);
	return ret;
}

/**
 * @brief Idempotent stop: clear running and wake the reactor.
 *
 * @param p poller instance.
 */
void iomux_stop(struct iomux_poller *p)
{
	if (p == NULL)
		return;
	atomic_store(&p->stopping, 1);
	atomic_store(&p->running, 0);
	wake_fd(p->stop_fd);
}

/**
 * @brief Detect reactor/worker self-call to avoid a join deadlock.
 *
 * @param p poller instance.
 * @return 0 on success, negative errno on failure.
 */
static int called_from_worker(struct iomux_poller *p)
{
	uint32_t i;
	pthread_t self = pthread_self();

	if (p->reactor_tid_valid &&
	    pthread_equal(self, p->reactor_tid))
		return 1;
	if (p->workers == NULL)
		return 0;
	for (i = 0u; i < p->nworkers; i++) {
		if (p->workers[i].started &&
		    pthread_equal(self, p->workers[i].tid))
			return 1;
	}
	return 0;
}

/**
 * @brief Stop, join workers, release memory; -EDEADLK on self-call.
 *
 * @param p poller instance.
 * @return 0 on success, negative errno on failure.
 */
int iomux_free(struct iomux_poller *p)
{
	uint32_t waited_ms = 0u;

	if (p == NULL)
		return 0;

	if (called_from_worker(p)) {
		iomux_log(p, IOMUX_LOG_ERROR,
			  "iomux_free from reactor/worker thread rejected");
		return -EDEADLK;
	}

	iomux_stop(p);
	while (atomic_load(&p->running) && waited_ms < IOMUX_FREE_WAIT_MS) {
		usleep(IOMUX_FREE_WAIT_STEP_US);
		waited_ms += IOMUX_FREE_WAIT_STEP_US / 1000u;
	}

	iomux_worker_stop(p);
	iomux_worker_join(p);

	(void)close(p->epfd);
	(void)close(p->stop_fd);
	(void)close(p->comp_fd);
	pthread_cond_destroy(&p->cmd_cv);
	pthread_mutex_destroy(&p->cmd_mu);

	free(p->watchers);
	free(p->wheel);
	free(p->buckets);
	iomux_timer_destroy(p);
	free(p);
	return 0;
}

/**
 * @brief Validate config, create epoll/eventfds, preallocate, start workers.
 *
 * @param cfg configuration; NULL selects defaults.
 * @return New poller, or NULL on failure.
 */
struct iomux_poller *iomux_new(const struct iomux_cfg *cfg)
{
	struct iomux_poller *p;
	struct epoll_event ev;
	int rc;

	p = calloc(1, sizeof(*p));
	if (p == NULL)
		return NULL;

	if (cfg != NULL)
		p->cfg = *cfg;
	else
		iomux_cfg_default(&p->cfg);

	if (!cfg_valid(&p->cfg)) {
		free(p);
		return NULL;
	}

	atomic_init(&p->running, 0);
	atomic_init(&p->stopping, 0);
	atomic_init(&p->started, 0);
	atomic_init(&p->rr, 0u);
	atomic_init(&p->reactor_tid_valid, 0);

	p->epfd = -1;
	p->stop_fd = -1;
	p->comp_fd = -1;
	p->nworkers = p->cfg.nworkers;

	p->epfd = epoll_create1(EPOLL_CLOEXEC);
	if (p->epfd < 0)
		goto fail;

	p->stop_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
	if (p->stop_fd < 0)
		goto fail;

	p->comp_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
	if (p->comp_fd < 0)
		goto fail;

	memset(&ev, 0, sizeof(ev));
	ev.events = EPOLLIN;
	ev.data.ptr = &p->stop_tag;
	if (epoll_ctl(p->epfd, EPOLL_CTL_ADD, p->stop_fd, &ev) != 0)
		goto fail;

	ev.data.ptr = &p->comp_tag;
	if (epoll_ctl(p->epfd, EPOLL_CTL_ADD, p->comp_fd, &ev) != 0)
		goto fail;

	rc = pthread_mutex_init(&p->cmd_mu, NULL);
	if (rc != 0)
		goto fail;
	rc = pthread_cond_init(&p->cmd_cv, NULL);
	if (rc != 0)
		goto fail_mutex;

	rc = alloc_watchers(p);
	if (rc != 0)
		goto fail_cond;

	rc = iomux_timer_init(p);
	if (rc != 0)
		goto fail_watchers;

	rc = iomux_worker_start(p);
	if (rc != 0)
		goto fail_timers;

	return p;

fail_timers:
	iomux_timer_destroy(p);
fail_watchers:
	iomux_timer_destroy(p);
	free(p->watchers);
	free(p->wheel);
	free(p->buckets);
fail_cond:
	pthread_cond_destroy(&p->cmd_cv);
fail_mutex:
	pthread_mutex_destroy(&p->cmd_mu);
fail:
	if (p->epfd >= 0)
		(void)close(p->epfd);
	if (p->stop_fd >= 0)
		(void)close(p->stop_fd);
	if (p->comp_fd >= 0)
		(void)close(p->comp_fd);
	free(p);
	return NULL;
}

/**
 * @brief Validate arguments, then submit an ADD command.
 *
 * @param p poller instance.
 * @param fd registered descriptor.
 * @param events EPOLL* interest mask.
 * @param cb callback executed on a worker.
 * @param arg opaque user pointer.
 * @param bind optional worker binding; NULL means round-robin.
 * @return 0 on success, negative errno on failure.
 */

int iomux_add(struct iomux_poller *p, int fd, uint32_t events,
	      iomux_cb cb, void *arg, const struct iomux_bind *bind)
{
	struct iomux_cmd c;

	if (p == NULL || fd < 0 || events == 0u || cb == NULL)
		return -EINVAL;

	memset(&c, 0, sizeof(c));
	c.op = IOMUX_CMD_ADD;
	c.fd = fd;
	c.events = events;
	c.cb = cb;
	c.arg = arg;
	if (bind != NULL)
		c.bind = *bind;

	return call_cmd(p, &c);
}

/**
 * @brief Validate arguments, then submit a MOD command.
 *
 * @param p poller instance.
 * @param fd registered descriptor.
 * @param events EPOLL* interest mask.
 * @return 0 on success, negative errno on failure.
 */
int iomux_mod(struct iomux_poller *p, int fd, uint32_t events)
{
	struct iomux_cmd c;

	if (p == NULL || fd < 0 || events == 0u)
		return -EINVAL;

	memset(&c, 0, sizeof(c));
	c.op = IOMUX_CMD_MOD;
	c.fd = fd;
	c.events = events;
	return call_cmd(p, &c);
}

/**
 * @brief Validate arguments, then submit a DEL command.
 *
 * @param p poller instance.
 * @param fd registered descriptor.
 * @return 0 on success, negative errno on failure.
 */
int iomux_del(struct iomux_poller *p, int fd)
{
	struct iomux_cmd c;

	if (p == NULL || fd < 0)
		return -EINVAL;

	memset(&c, 0, sizeof(c));
	c.op = IOMUX_CMD_DEL;
	c.fd = fd;
	return call_cmd(p, &c);
}

/**
 * @brief Validate arguments, then submit a timer ADD.
 *
 * @param p poller instance.
 * @param delay_ms first expiry in milliseconds.
 * @param interval_ms 0 for one-shot, >0 for periodic.
 * @param cb callback executed on a worker.
 * @param arg opaque user pointer.
 * @param err out: 0 on success, -errno on failure.
 * @return Timer id, or 0 on failure (see err).
 */
uint64_t iomux_timer_add(struct iomux_poller *p, uint32_t delay_ms,
			 uint32_t interval_ms, iomux_timer_cb cb,
			 void *arg, int *err)
{
	struct iomux_cmd c;
	int rc;

	if (p == NULL || cb == NULL) {
		if (err != NULL)
			*err = -EINVAL;
		return 0u;
	}

	memset(&c, 0, sizeof(c));
	c.op = IOMUX_CMD_TIMER_ADD;
	c.delay_ms = delay_ms;
	c.interval_ms = interval_ms;
	c.tcb = cb;
	c.arg = arg;

	rc = call_cmd(p, &c);
	if (err != NULL)
		*err = rc;
	return (rc == 0) ? c.timer_id : 0u;
}

/**
 * @brief Cancel by id; -ENOENT when it already fired or was cancelled.
 *
 * @param p poller instance.
 * @param id input parameter.
 * @return 0 on success, negative errno on failure.
 */
int iomux_timer_cancel(struct iomux_poller *p, uint64_t id)
{
	struct iomux_cmd c;

	if (p == NULL || id == 0u)
		return -EINVAL;

	memset(&c, 0, sizeof(c));
	c.op = IOMUX_CMD_TIMER_CANCEL;
	c.timer_id = id;
	return call_cmd(p, &c);
}

/**
 * @brief Return the internal counter block; owned by the poller.
 *
 * @param p poller instance.
 * @return Internal counters; owned by the poller.
 */
const struct iomux_stats *iomux_stats(struct iomux_poller *p)
{
	return (p != NULL) ? &p->stats : NULL;
}

/**
 * @brief Map library error codes to static English strings.
 *
 * @param err out: 0 on success, -errno on failure.
 * @return Static string; never NULL.
 */
const char *iomux_strerror(int err)
{
	switch (err) {
	case 0:
		return "ok";
	case -EINVAL:
		return "invalid argument";
	case -ENOMEM:
		return "out of memory";
	case -ENOSPC:
		return "no slots left";
	case -EEXIST:
		return "already registered";
	case -ENOENT:
		return "not found";
	case -EBUSY:
		return "busy";
	case -ESHUTDOWN:
		return "poller shut down";
	case -EAGAIN:
		return "queue full";
	default:
		return "unknown error";
	}
}
