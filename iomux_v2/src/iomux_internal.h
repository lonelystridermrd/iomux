/*
 * iomux_internal.h - internal types shared by the iomux translation units.
 * Not installed; do not include from outside the library.
 */

#ifndef IOMUX_INTERNAL_H
#define IOMUX_INTERNAL_H

#include "iomux.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <sys/epoll.h>
#include <sys/types.h>

#define IOMUX_CMD_CAP	256u

enum iomux_state {
	IOMUX_IDLE = 0,
	IOMUX_INFLIGHT,
	IOMUX_DEAD
};

enum iomux_task_type {
	IOMUX_TASK_FD = 1,
	IOMUX_TASK_TIMER
};

struct iomux_task {
	uint32_t type;
	uint32_t worker;
	int fd;
	uint32_t events;
	void *obj;		/* watcher or timer */
};

/* Single-producer/single-consumer ring. Capacity is a power of two. */
struct iomux_ring {
	struct iomux_task *buf;
	uint32_t mask;
	_Atomic uint32_t head;	/* consumer */
	_Atomic uint32_t tail;	/* producer */
};

struct iomux_watcher {
	int fd;
	uint32_t events;
	uint8_t state;
	iomux_cb cb;
	void *arg;
	iomux_hash_fn hash;
	void *hash_arg;
	uint32_t worker;
	uint8_t has_ident;
	dev_t fdev;
	ino_t fino;
	struct iomux_watcher *hnext;
	struct iomux_watcher *free_next;
	struct iomux_watcher *pend_next;
};

struct iomux_timer {
	uint64_t id;
	uint64_t expire_ms;
	uint32_t interval_ms;
	iomux_timer_cb cb;
	void *arg;
	uint8_t state;
	uint32_t heap_idx;
};

struct iomux_worker {
	struct iomux_poller *p;
	pthread_t tid;
	int wake_fd;
	int started;
	uint32_t index;
	_Atomic int stop;
	struct iomux_ring in;	/* reactor -> worker */
	struct iomux_ring out;	/* worker -> reactor */
	uint64_t processed;
};

enum iomux_cmd_op {
	IOMUX_CMD_ADD = 1,
	IOMUX_CMD_MOD,
	IOMUX_CMD_DEL,
	IOMUX_CMD_TIMER_ADD,
	IOMUX_CMD_TIMER_CANCEL,
	IOMUX_CMD_STOP
};

struct iomux_cmd {
	uint32_t op;
	int fd;
	uint32_t events;
	iomux_cb cb;
	void *arg;
	struct iomux_bind bind;
	uint32_t delay_ms;
	uint32_t interval_ms;
	iomux_timer_cb tcb;
	uint64_t timer_id;
	int result;
	int done;
};

struct iomux_poller {
	struct iomux_cfg cfg;
	int epfd;
	int stop_fd;
	int comp_fd;
	int stop_tag;		/* address used as epoll data.ptr sentinel */
	int comp_tag;

	struct iomux_worker *workers;
	uint32_t nworkers;

	/* fd table */
	struct iomux_watcher *watchers;
	uint32_t max_watchers;
	struct iomux_watcher **wheel;	/* free list stack */
	uint32_t nfree;
	struct iomux_watcher **buckets;
	uint32_t nbuckets;
	uint32_t bucket_mask;

	/* pending dispatches (queue was full) */
	struct iomux_watcher *pend_head;
	struct iomux_watcher *pend_tail;

	/* timers */
	struct iomux_timer *timers;
	uint32_t max_timers;
	struct iomux_timer **theap;
	uint32_t heap_len;
	struct iomux_timer **tfree;
	uint32_t ntfree;
	uint64_t next_timer_id;

	/* command queue (mutex protected, entries are caller-owned) */
	pthread_mutex_t cmd_mu;
	pthread_cond_t cmd_cv;
	struct iomux_cmd *cmds[IOMUX_CMD_CAP];
	uint32_t cmd_head;
	uint32_t cmd_tail;

	_Atomic int running;
	_Atomic int stopping;
	_Atomic int started;	/* run() has been called at least once */
	_Atomic uint32_t rr;

	pthread_t reactor_tid;
	atomic_int reactor_tid_valid;

	struct iomux_stats stats;
};

/* ring */
int iomux_ring_init(struct iomux_ring *r, uint32_t capacity);
void iomux_ring_destroy(struct iomux_ring *r);
int iomux_ring_push(struct iomux_ring *r, const struct iomux_task *t);
int iomux_ring_pop(struct iomux_ring *r, struct iomux_task *t);

/* timer */
int iomux_timer_init(struct iomux_poller *p);
void iomux_timer_destroy(struct iomux_poller *p);
int iomux_timer_push(struct iomux_poller *p, uint64_t delay_ms,
		     uint32_t interval_ms, iomux_timer_cb cb, void *arg,
		     uint64_t *id_out);
int iomux_timer_remove(struct iomux_poller *p, uint64_t id);
void iomux_timer_fire(struct iomux_poller *p, uint64_t now_ms);
int iomux_timer_next_ms(struct iomux_poller *p, uint64_t now_ms);
void iomux_timer_reap(struct iomux_poller *p, struct iomux_timer *t);

/* worker */
int iomux_worker_start(struct iomux_poller *p);
void iomux_worker_stop(struct iomux_poller *p);
void iomux_worker_join(struct iomux_poller *p);

/* poller helpers */
void iomux_log(struct iomux_poller *p, int level, const char *fmt, ...);
uint64_t iomux_now_ms(void);
int iomux_dispatch(struct iomux_poller *p, struct iomux_task *t);
void iomux_reap(struct iomux_poller *p, struct iomux_task *done);

#endif /* IOMUX_INTERNAL_H */
