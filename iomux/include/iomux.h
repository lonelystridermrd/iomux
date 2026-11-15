/**
 * @file iomux.h
 * @brief Embedded event multiplexer: epoll (level-triggered) + single reactor
 *        + configurable worker pool.
 *
 * Design notes:
 *  - One poller owns one epoll instance and one timer heap; the reactor thread
 *    is the only thread that touches them.
 *  - Business callbacks run in a worker pool. Each worker has its own SPSC
 *    queue fed by the reactor; completions return through a per-worker ring.
 *  - All fds are registered with EPOLLONESHOT (still level-triggered): the
 *    reactor disarms a watcher while its callback is in flight, and rearms it
 *    after completion. This keeps LT semantics without reactor spinning.
 *
 * Type conventions:
 *  - structs are never typedef'd; use "struct iomux_cfg" or
 *    "struct iomux_poller *".
 *  - function pointer typedefs use _cb / _fn suffixes, never _t.
 *  - integer aliases would use _t, but this API exposes none.
 *
 * Threading:
 *  - iomux_new/run/free are lifecycle calls and must be sequenced by the owner.
 *  - iomux_add/mod/del/timer_* may be called from any thread (serialized into
 *    the reactor through a command queue).
 */

#ifndef IOMUX_H
#define IOMUX_H

#include <stdarg.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define IOMUX_MAX_WORKERS	64u
#define IOMUX_MAX_EVENTS	4096u
#define IOMUX_MIN_QUEUE		64u

/**
 * @brief Opaque poller instance.
 *
 * Definition lives in src/iomux_internal.h. Callers only hold pointers and
 * must not allocate or inspect the object.
 */
struct iomux_poller;

/**
 * @brief IO callback, executed on a worker thread.
 *
 * @param fd     Registered descriptor that became ready.
 * @param events EPOLL* mask reported by epoll (EPOLLIN/EPOLLOUT/EPOLLERR/...).
 * @param arg    Opaque pointer supplied to iomux_add().
 *
 * @note Must not block for long: it occupies one worker. Must not call
 *       iomux_free() (rejected). iomux_add/del/timer_* are allowed here.
 */
typedef void (*iomux_cb)(int fd, uint32_t events, void *arg);

/**
 * @brief Timer callback, executed on a worker thread.
 *
 * @param arg Opaque pointer supplied to iomux_timer_add().
 *
 * @note Periodic timers drop a tick (no queueing, no catch-up) when the
 *       previous callback is still running.
 */
typedef void (*iomux_timer_cb)(void *arg);

/**
 * @brief Log sink. Called from any thread.
 *
 * @param level One of IOMUX_LOG_*.
 * @param fmt   printf-style format; never NULL.
 * @param ap    Argument list.
 *
 * @note Pass NULL as cfg.log to make the library silent. The default is
 *       silent: the library never writes to stdio on its own.
 */
typedef void (*iomux_log_cb)(int level, const char *fmt, va_list ap);

/** @brief Log levels passed to iomux_log_cb. */
enum iomux_log_level {
	IOMUX_LOG_ERROR = 0,
	IOMUX_LOG_WARN,
	IOMUX_LOG_INFO,
	IOMUX_LOG_DEBUG
};

/**
 * @brief Optional worker-affinity hash.
 *
 * @param fd  Registered descriptor.
 * @param arg Opaque pointer from struct iomux_bind.
 * @return Hash; the library maps it to worker index % nworkers.
 *
 * @note Called on the reactor thread for every event dispatch, so it must be
 *       cheap and must not block.
 */
typedef uint32_t (*iomux_hash_fn)(int fd, void *arg);

/**
 * @brief Per-registration worker binding.
 *
 * When @c hash is NULL the reactor round-robins across workers.
 */
struct iomux_bind {
	iomux_hash_fn hash;	/**< NULL means round-robin. */
	void *arg;		/**< Passed to hash() unchanged. */
};

/**
 * @brief Poller configuration.
 *
 * Zero-initialise then call iomux_cfg_default(), or fill every field.
 * Invalid values make iomux_new() return NULL.
 */
struct iomux_cfg {
	uint32_t nworkers;	/**< Worker threads, 1..IOMUX_MAX_WORKERS. */
	uint32_t max_events;	/**< epoll_wait batch, 1..IOMUX_MAX_EVENTS. */
	uint32_t queue_depth;	/**< Per-worker queue entries, power of two >= 64. */
	uint32_t max_watchers;	/**< Preallocated watcher slots, >= 16. */
	uint32_t timer_capacity;/**< Preallocated timer slots, >= 16. */
	iomux_log_cb log;	/**< Optional; NULL = silent. */
};

/**
 * @brief Runtime counters. Updated by the reactor; read-only to callers.
 *
 * Values are monotonic and may be read without locking; a snapshot may lag
 * the reactor by one loop iteration.
 */
struct iomux_stats {
	uint64_t dispatched;	/**< Callbacks queued to workers. */
	uint64_t rearmed;	/**< Watchers rearmed after completion. */
	uint64_t queue_full;	/**< Dispatches deferred because a queue was full. */
	uint64_t timer_fired;	/**< Timer callbacks queued. */
	uint64_t timer_skipped;	/**< Periodic ticks dropped (previous still running). */
	uint64_t epoll_events;	/**< Total events returned by epoll_wait. */
	uint64_t epoll_batches;	/**< epoll_wait calls. */
	uint64_t errors;	/**< Internal error count (rearm failures, epoll errors). */
};

/**
 * @brief Fill @p cfg with safe defaults.
 *
 * @param cfg Must not be NULL.
 *
 * Defaults: 2 workers, 512 events, 1024 queue depth, 4096 watchers,
 * 1024 timers, silent log.
 */
void iomux_cfg_default(struct iomux_cfg *cfg);

/**
 * @brief Create a poller and start its worker threads.
 *
 * @param cfg Configuration; NULL means "use defaults".
 * @return New poller, or NULL on invalid cfg / allocation / thread failure.
 *
 * @note Worker threads start here, so a poller must be freed even if
 *       iomux_run() is never called.
 */
struct iomux_poller *iomux_new(const struct iomux_cfg *cfg);

/**
 * @brief Run the reactor loop; blocks until iomux_stop() or a fatal error.
 *
 * @param p Poller; must not be NULL.
 * @return 0 after a normal stop, or -errno on fatal epoll failure.
 *
 * @note Can be called only once per poller: a second call returns -EBUSY.
 *       Must be invoked from the thread that will block (not a worker).
 */
int iomux_run(struct iomux_poller *p);

/**
 * @brief Request the reactor to stop.
 *
 * @param p Poller; NULL is ignored.
 *
 * @note Safe from any thread. Idempotent. Does not join; call iomux_free()
 *       after iomux_run() returns.
 */
void iomux_stop(struct iomux_poller *p);

/**
 * @brief Stop, join workers and release all memory.
 *
 * @param p Poller; NULL is ignored.
 *
 * @note Must NOT be called from a reactor or worker thread: such a call is
 *       rejected with a log entry (self-join would deadlock). Must be called
 *       after iomux_run() returned; it waits up to 5s for the reactor.
 */
void iomux_free(struct iomux_poller *p);

/**
 * @brief Register @p fd for EPOLL events.
 *
 * @param p      Poller; must not be NULL.
 * @param fd     Descriptor, >= 0; caller keeps ownership and must not close
 *               it before iomux_del().
 * @param events Non-zero EPOLL* mask.
 * @param cb     Non-NULL callback, runs on a worker.
 * @param arg    Opaque pointer passed to @p cb; may be NULL.
 * @param bind   Optional worker binding; NULL means round-robin.
 * @return 0, -EINVAL on bad args, -EEXIST if already registered,
 *         -ENOSPC if watcher slots are exhausted, -ESHUTDOWN after stop.
 *
 * @note The fd is armed with EPOLLONESHOT and rearmed after each callback.
 *       Calling it before iomux_run() is allowed and executes inline.
 */
int iomux_add(struct iomux_poller *p, int fd, uint32_t events,
	      iomux_cb cb, void *arg, const struct iomux_bind *bind);

/**
 * @brief Replace the interest mask of a registered fd.
 *
 * @return 0, -EINVAL, -ENOENT if not registered, -EBUSY while a callback for
 *         @p fd is in flight, -ESHUTDOWN after stop.
 *
 * @note Safe from callbacks. Changes take effect on the next rearm.
 */
int iomux_mod(struct iomux_poller *p, int fd, uint32_t events);

/**
 * @brief Unregister a descriptor.
 *
 * @return 0, -EINVAL, -ENOENT if unknown, -ESHUTDOWN after stop.
 *
 * @note Idempotent-safe. Allowed inside callbacks. Must be called before
 *       close(): fd numbers get reused and the library cannot detect that.
 *       If a callback is in flight, the watcher is reclaimed after it returns.
 */
int iomux_del(struct iomux_poller *p, int fd);

/**
 * @brief Add a one-shot or periodic timer.
 *
 * @param p           Poller; must not be NULL.
 * @param delay_ms    First expiry, in milliseconds; 0 fires on the next tick.
 * @param interval_ms 0 = one-shot; >0 = periodic.
 * @param cb          Non-NULL timer callback, runs on a worker.
 * @param arg         Opaque pointer passed to @p cb; may be NULL.
 * @param err         Optional; set to 0 on success or -errno on failure.
 * @return Timer id > 0 on success, 0 on failure (see @p err).
 *
 * @note Periodic ticks are dropped, not queued, when the previous callback is
 *       still running. A one-shot timer cannot be cancelled once it fired.
 */
uint64_t iomux_timer_add(struct iomux_poller *p, uint32_t delay_ms,
			 uint32_t interval_ms, iomux_timer_cb cb,
			 void *arg, int *err);

/**
 * @brief Cancel a timer by id.
 *
 * @return 0, -EINVAL on bad args, -ENOENT if unknown (already fired or
 *         cancelled), -ESHUTDOWN after stop.
 *
 * @note Cancelling a periodic timer that is currently running marks it dead;
 *       the slot is released when the callback returns.
 */
int iomux_timer_cancel(struct iomux_poller *p, uint64_t id);

/**
 * @brief Read runtime counters.
 *
 * @param p Poller; may be NULL.
 * @return Pointer to the internal counters, or NULL when @p p is NULL.
 *
 * @note The pointer is owned by the poller and becomes invalid after
 *       iomux_free(). Values may change while the caller reads them.
 */
const struct iomux_stats *iomux_stats(struct iomux_poller *p);

/**
 * @brief Text for a negative error code.
 *
 * @param err 0 or a negative errno value used by this library.
 * @return Static string; never NULL.
 */
const char *iomux_strerror(int err);

#ifdef __cplusplus
}
#endif

#endif /* IOMUX_H */
