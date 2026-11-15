/*
 * iomux_timer.c - per-poller timer min-heap.
 *
 * One-shot timers are freed on completion. Periodic timers are re-inserted at
 * fire time; if the previous callback is still in flight, that tick is dropped
 * (timer_skipped) instead of queued, so callbacks never overlap.
 */

#include "iomux_internal.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

/**
 * @brief Min-heap order: expire time, then id for stability.
 *
 * @param a input parameter.
 * @param b input parameter.
 * @return 0 on success, negative errno on failure.
 */
static int heap_less(const struct iomux_timer *a, const struct iomux_timer *b)
{
	if (a->expire_ms != b->expire_ms)
		return a->expire_ms < b->expire_ms;
	return a->id < b->id;
}

/**
 * @brief Swap two heap slots and fix their heap_idx back-pointers.
 *
 * @param p poller instance.
 * @param a input parameter.
 * @param b input parameter.
 */
static void heap_swap(struct iomux_poller *p, uint32_t a, uint32_t b)
{
	struct iomux_timer *ta = p->theap[a];
	struct iomux_timer *tb = p->theap[b];

	p->theap[a] = tb;
	tb->heap_idx = a;
	p->theap[b] = ta;
	ta->heap_idx = b;
}

/**
 * @brief Sift a node up after insert.
 *
 * @param p poller instance.
 * @param i input parameter.
 */
static void heap_up(struct iomux_poller *p, uint32_t i)
{
	while (i > 0u) {
		uint32_t parent = (i - 1u) / 2u;

		if (!heap_less(p->theap[i], p->theap[parent]))
			break;
		heap_swap(p, i, parent);
		i = parent;
	}
}

/**
 * @brief Sift a node down after pop/remove.
 *
 * @param p poller instance.
 * @param i input parameter.
 */
static void heap_down(struct iomux_poller *p, uint32_t i)
{
	for (;;) {
		uint32_t left = 2u * i + 1u;
		uint32_t right = left + 1u;
		uint32_t best = i;

		if (left < p->heap_len &&
		    heap_less(p->theap[left], p->theap[best]))
			best = left;
		if (right < p->heap_len &&
		    heap_less(p->theap[right], p->theap[best]))
			best = right;
		if (best == i)
			break;
		heap_swap(p, i, best);
		i = best;
	}
}

/**
 * @brief Insert and record heap_idx; O(log n).
 *
 * @param p poller instance.
 * @param t task descriptor.
 */
static void heap_insert(struct iomux_poller *p, struct iomux_timer *t)
{
	p->theap[p->heap_len] = t;
	t->heap_idx = p->heap_len;
	p->heap_len++;
	heap_up(p, t->heap_idx);
}

/**
 * @brief Remove by index; O(log n) thanks to heap_idx.
 *
 * @param p poller instance.
 * @param i input parameter.
 */
static void heap_remove_at(struct iomux_poller *p, uint32_t i)
{
	uint32_t last = p->heap_len - 1u;

	if (i != last)
		heap_swap(p, i, last);
	p->heap_len--;
	if (i < p->heap_len) {
		heap_up(p, i);
		heap_down(p, i);
	}
}

/**
 * @brief Take a zeroed timer slot from the preallocated pool.
 *
 * @param p poller instance.
 */
static struct iomux_timer *timer_alloc(struct iomux_poller *p)
{
	struct iomux_timer *t;

	if (p->ntfree == 0u)
		return NULL;
	t = p->tfree[--p->ntfree];
	memset(t, 0, sizeof(*t));
	t->state = IOMUX_IDLE;
	return t;
}

/**
 * @brief Return a timer slot; callback must have finished.
 *
 * @param p poller instance.
 * @param t task descriptor.
 */
static void timer_free(struct iomux_poller *p, struct iomux_timer *t)
{
	t->state = IOMUX_DEAD;
	t->cb = NULL;
	t->arg = NULL;
	if (p->ntfree < p->max_timers)
		p->tfree[p->ntfree++] = t;
}

/**
 * @brief Preallocate timer slots and the heap array.
 *
 * @param p poller instance.
 * @return 0 on success, negative errno on failure.
 */
int iomux_timer_init(struct iomux_poller *p)
{
	uint32_t i;

	p->max_timers = p->cfg.timer_capacity;
	p->timers = calloc(p->max_timers, sizeof(*p->timers));
	p->theap = calloc(p->max_timers, sizeof(*p->theap));
	p->tfree = calloc(p->max_timers, sizeof(*p->tfree));
	if (p->timers == NULL || p->theap == NULL || p->tfree == NULL)
		return -ENOMEM;

	for (i = 0u; i < p->max_timers; i++)
		p->tfree[i] = &p->timers[i];
	p->ntfree = p->max_timers;
	p->heap_len = 0u;
	p->next_timer_id = 0u;
	return 0;
}

/**
 * @brief Release timer storage.
 *
 * @param p poller instance.
 */
void iomux_timer_destroy(struct iomux_poller *p)
{
	free(p->timers);
	free(p->theap);
	free(p->tfree);
	p->timers = NULL;
	p->theap = NULL;
	p->tfree = NULL;
	p->heap_len = 0u;
	p->ntfree = 0u;
}

/**
 * @brief Validate arguments, take a slot, and insert into the expiry heap.
 *
 * @param p poller instance.
 * @param delay_ms first expiry in milliseconds.
 * @param interval_ms 0 for one-shot, >0 for periodic.
 * @param cb callback executed on a worker.
 * @param arg opaque user pointer.
 * @param id_out out: allocated timer id.
 * @return 0 on success, negative errno on failure.
 */
int iomux_timer_push(struct iomux_poller *p, uint64_t delay_ms,
		     uint32_t interval_ms, iomux_timer_cb cb, void *arg,
		     uint64_t *id_out)
{
	struct iomux_timer *t;

	if (p == NULL || cb == NULL || id_out == NULL)
		return -EINVAL;

	t = timer_alloc(p);
	if (t == NULL)
		return -ENOSPC;

	p->next_timer_id++;
	if (p->next_timer_id == 0u)
		p->next_timer_id++;
	t->id = p->next_timer_id;
	t->expire_ms = iomux_now_ms() + delay_ms;
	t->interval_ms = interval_ms;
	t->cb = cb;
	t->arg = arg;
	t->state = IOMUX_IDLE;
	heap_insert(p, t);

	*id_out = t->id;
	return 0;
}

/**
 * @brief Scan the heap by id; ids are not indexed separately.
 *
 * @param p poller instance.
 * @param id input parameter.
 * @return 0 on success, negative errno on failure.
 */
int iomux_timer_remove(struct iomux_poller *p, uint64_t id)
{
	uint32_t i;

	if (p == NULL || id == 0u)
		return -EINVAL;

	for (i = 0u; i < p->heap_len; i++) {
		struct iomux_timer *t = p->theap[i];

		if (t->id != id)
			continue;
		heap_remove_at(p, i);
		if (t->state == IOMUX_INFLIGHT)
			t->state = IOMUX_DEAD;
		else
			timer_free(p, t);
		return 0;
	}
	return -ENOENT;
}

/**
 * @brief Pop due timers, reinsert periodic ones, drop a tick while INFLIGHT.
 *
 * @param p poller instance.
 * @param now_ms input parameter.
 */
void iomux_timer_fire(struct iomux_poller *p, uint64_t now_ms)
{
	while (p->heap_len > 0u && p->theap[0]->expire_ms <= now_ms) {
		struct iomux_timer *t = p->theap[0];
		struct iomux_task task;

		heap_remove_at(p, 0u);

		if (t->state == IOMUX_INFLIGHT) {
			p->stats.timer_skipped++;
			if (t->interval_ms > 0u) {
				t->expire_ms = now_ms + t->interval_ms;
				heap_insert(p, t);
			}
			continue;
		}

		if (t->interval_ms > 0u) {
			t->expire_ms = now_ms + t->interval_ms;
			heap_insert(p, t);
		}

		task.type = IOMUX_TASK_TIMER;
		task.worker = atomic_fetch_add(&p->rr, 1u) % p->nworkers;
		task.fd = -1;
		task.events = 0u;
		task.obj = t;

		t->state = IOMUX_INFLIGHT;
		if (iomux_dispatch(p, &task) != 0) {
			p->stats.queue_full++;
			t->state = IOMUX_IDLE;
			if (t->interval_ms == 0u) {
				t->expire_ms = now_ms + 1u;
				heap_insert(p, t);
			}
		}
	}
}

/**
 * @brief Timeout for epoll_wait; -1 when no timer is armed.
 *
 * @param p poller instance.
 * @param now_ms input parameter.
 * @return 0 on success, negative errno on failure.
 */
int iomux_timer_next_ms(struct iomux_poller *p, uint64_t now_ms)
{
	uint64_t now;
	uint64_t delta;

	if (p->heap_len == 0u)
		return -1;

	now = (now_ms != 0u) ? now_ms : iomux_now_ms();
	if (p->theap[0]->expire_ms <= now)
		return 0;

	delta = p->theap[0]->expire_ms - now;
	if (delta > 2147483647ull)
		return 2147483647;
	return (int)delta;
}

/**
 * @brief Free one-shot timers, idle periodic ones.
 *
 * @param p poller instance.
 * @param t task descriptor.
 */
void iomux_timer_reap(struct iomux_poller *p, struct iomux_timer *t)
{
	if (t->state == IOMUX_DEAD || t->interval_ms == 0u) {
		timer_free(p, t);
		return;
	}
	t->state = IOMUX_IDLE;
}
