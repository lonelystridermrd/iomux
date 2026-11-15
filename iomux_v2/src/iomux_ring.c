/*
 * iomux_ring.c - SPSC ring buffer used for reactor<->worker hand-off.
 */

#include "iomux_internal.h"

#include <errno.h>
#include <stdlib.h>

/**
 * @brief Allocate an SPSC ring; capacity must be a power of two.
 *
 * @param r ring buffer.
 * @param capacity input parameter.
 * @return 0 on success, negative errno on failure.
 */
int iomux_ring_init(struct iomux_ring *r, uint32_t capacity)
{
	if (r == NULL || capacity == 0u ||
	    (capacity & (capacity - 1u)) != 0u)
		return -EINVAL;

	r->buf = calloc(capacity, sizeof(*r->buf));
	if (r->buf == NULL)
		return -ENOMEM;

	r->mask = capacity - 1u;
	atomic_init(&r->head, 0u);
	atomic_init(&r->tail, 0u);
	return 0;
}

/**
 * @brief Release the ring buffer.
 *
 * @param r ring buffer.
 */
void iomux_ring_destroy(struct iomux_ring *r)
{
	free(r->buf);
	r->buf = NULL;
	r->mask = 0u;
}

/**
 * @brief Producer side; returns -EAGAIN when the ring is full.
 *
 * @param r ring buffer.
 * @param t task descriptor.
 * @return 0 on success, negative errno on failure.
 */
int iomux_ring_push(struct iomux_ring *r, const struct iomux_task *t)
{
	uint32_t tail = atomic_load_explicit(&r->tail, memory_order_relaxed);
	uint32_t head = atomic_load_explicit(&r->head, memory_order_acquire);

	if ((tail - head) >= (r->mask + 1u))
		return -EAGAIN;

	r->buf[tail & r->mask] = *t;
	atomic_store_explicit(&r->tail, tail + 1u, memory_order_release);
	return 0;
}

/**
 * @brief Consumer side; release/acquire pairs with the producer.
 *
 * @param r ring buffer.
 * @param t task descriptor.
 * @return 0 on success, negative errno on failure.
 */
int iomux_ring_pop(struct iomux_ring *r, struct iomux_task *t)
{
	uint32_t head = atomic_load_explicit(&r->head, memory_order_relaxed);
	uint32_t tail = atomic_load_explicit(&r->tail, memory_order_acquire);

	if (head == tail)
		return -ENOENT;

	*t = r->buf[head & r->mask];
	atomic_store_explicit(&r->head, head + 1u, memory_order_release);
	return 0;
}
