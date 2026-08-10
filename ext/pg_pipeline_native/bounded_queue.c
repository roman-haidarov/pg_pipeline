#include "ruby.h"
#include "ruby/fiber/scheduler.h"
#include <stdlib.h>
#include <string.h>

static VALUE cBoundedQueue;
static VALUE cShutdownError;
static ID id_alive_p;

typedef struct {
    VALUE fiber;
    VALUE scheduler;
} bq_waiter_t;

typedef struct {
    VALUE *items;
    size_t capacity; /* power of two */
    size_t mask;
    size_t limit;
    size_t head;
    size_t length;

    bq_waiter_t *producers;
    size_t prod_len;
    size_t prod_cap;

    bq_waiter_t *consumers;
    size_t cons_len;
    size_t cons_cap;

    VALUE close_error;
    int closed;
} bq_t;

static size_t bq_next_pow2(size_t n) {
    size_t p = 1;
    while (p < n) {
        if (p > (SIZE_MAX / 2))
            rb_raise(rb_eArgError, "BoundedQueue limit is too large");
        p <<= 1;
    }
    return p;
}

static void bq_mark(void *ptr) {
    bq_t *q = ptr;
    if (!q)
        return;
    for (size_t i = 0; i < q->length; i++) {
        size_t pos = (q->head + i) & q->mask;
        rb_gc_mark(q->items[pos]);
    }
    for (size_t i = 0; i < q->prod_len; i++) {
        rb_gc_mark(q->producers[i].fiber);
        rb_gc_mark(q->producers[i].scheduler);
    }
    for (size_t i = 0; i < q->cons_len; i++) {
        rb_gc_mark(q->consumers[i].fiber);
        rb_gc_mark(q->consumers[i].scheduler);
    }
    rb_gc_mark(q->close_error);
}

static void bq_free(void *ptr) {
    bq_t *q = ptr;
    if (!q)
        return;
    xfree(q->items);
    xfree(q->producers);
    xfree(q->consumers);
    xfree(q);
}

static size_t bq_memsize(const void *ptr) {
    const bq_t *q = ptr;
    if (!q)
        return 0;
    return sizeof(*q) + sizeof(VALUE) * q->capacity +
           sizeof(bq_waiter_t) * (q->prod_cap + q->cons_cap);
}

static const rb_data_type_t bq_type = {"PgPipeline::BoundedQueue",
                                       {bq_mark, bq_free, bq_memsize, NULL, {NULL}},
                                       NULL,
                                       NULL,
                                       RUBY_TYPED_FREE_IMMEDIATELY};

static bq_t *bq_get(VALUE self) {
    bq_t *q;
    TypedData_Get_Struct(self, bq_t, &bq_type, q);
    return q;
}

static VALUE bq_alloc(VALUE klass) {
    bq_t *q;
    VALUE obj = TypedData_Make_Struct(klass, bq_t, &bq_type, q);
    memset(q, 0, sizeof(*q));
    q->close_error = Qnil;
    return obj;
}

static void bq_waiter_push(bq_waiter_t **list, size_t *len, size_t *cap, VALUE fiber,
                           VALUE scheduler) {
    if (*len >= *cap) {
        size_t ncap = *cap ? *cap * 2 : 4;
        bq_waiter_t *grown = *list;
        REALLOC_N(grown, bq_waiter_t, ncap);
        *list = grown;
        *cap = ncap;
    }
    (*list)[*len].fiber = fiber;
    (*list)[*len].scheduler = scheduler;
    (*len)++;
}

static void bq_wake_one(VALUE blocker, bq_waiter_t **list, size_t *len) {
    if (*len == 0)
        return;
    bq_waiter_t w = (*list)[0];
    memmove(*list, *list + 1, sizeof(bq_waiter_t) * (*len - 1));
    (*len)--;
    if (!NIL_P(w.fiber) && !NIL_P(w.scheduler) && RTEST(rb_funcall(w.fiber, id_alive_p, 0))) {
        rb_fiber_scheduler_unblock(w.scheduler, blocker, w.fiber);
    }
}

/*
 * `unblock` is caller-supplied Ruby: a Fiber::Scheduler is allowed to resume the
 * woken fiber inline, and that fiber can re-enter enqueue/dequeue, which pushes
 * new waiters and can REALLOC the array under us. So detach the whole waiter set
 * into a Ruby array first, zero the length, and only then run any Ruby. Waiters
 * that arrive during the walk stay queued instead of being silently dropped by a
 * trailing `*len = 0`.
 */
static void bq_wake_all(VALUE blocker, bq_waiter_t **list, size_t *len) {
    size_t n = *len;
    if (n == 0)
        return;

    VALUE detached = rb_ary_new_capa((long)(n * 2));
    for (size_t i = 0; i < n; i++) {
        rb_ary_push(detached, (*list)[i].fiber);
        rb_ary_push(detached, (*list)[i].scheduler);
    }
    *len = 0;

    for (size_t i = 0; i < n; i++) {
        VALUE fiber = RARRAY_AREF(detached, (long)(i * 2));
        VALUE scheduler = RARRAY_AREF(detached, (long)(i * 2 + 1));
        if (!NIL_P(fiber) && !NIL_P(scheduler) && RTEST(rb_funcall(fiber, id_alive_p, 0))) {
            rb_fiber_scheduler_unblock(scheduler, blocker, fiber);
        }
    }
    RB_GC_GUARD(detached);
}

/*
 * Re-raise the exception the queue was closed with, as-is. Rebuilding it from
 * (class, message) used to drop the backtrace and `cause`, and broke outright
 * for any error class whose #initialize does not take exactly one String.
 */
NORETURN(static void bq_raise_closed(bq_t *q));
static void bq_raise_closed(bq_t *q) {
    VALUE err = q->close_error;
    if (NIL_P(err) || !rb_obj_is_kind_of(err, rb_eException))
        err = rb_exc_new_cstr(cShutdownError, "queue closed");
    rb_exc_raise(err);
}

typedef struct {
    VALUE self;
    VALUE scheduler;
    VALUE fiber;
    bq_t *q;
    bq_waiter_t **list;
    size_t *len;
    int completed;
} bq_park_ctx_t;

static int bq_waiter_index(bq_waiter_t *list, size_t len, VALUE fiber) {
    for (size_t i = 0; i < len; i++) {
        if (list[i].fiber == fiber)
            return (int)i;
    }
    return -1;
}

static VALUE bq_park_body(VALUE ptr) {
    bq_park_ctx_t *ctx = (bq_park_ctx_t *)ptr;
    while (bq_waiter_index(*ctx->list, *ctx->len, ctx->fiber) >= 0) {
        rb_fiber_scheduler_block(ctx->scheduler, ctx->self, Qnil);
    }
    ctx->completed = 1;
    return Qnil;
}

static VALUE bq_park_ensure(VALUE ptr) {
    bq_park_ctx_t *ctx = (bq_park_ctx_t *)ptr;
    int idx = bq_waiter_index(*ctx->list, *ctx->len, ctx->fiber);
    if (idx >= 0) {
        memmove(*ctx->list + idx, *ctx->list + idx + 1,
                sizeof(bq_waiter_t) * (*ctx->len - (size_t)idx - 1));
        (*ctx->len)--;
        if (!ctx->completed && !ctx->q->closed && *ctx->len > 0)
            bq_wake_one(ctx->self, ctx->list, ctx->len);
    }
    return Qnil;
}

static void bq_park(VALUE self, bq_t *q, bq_waiter_t **list, size_t *len, size_t *cap) {
    VALUE scheduler = rb_fiber_scheduler_current();
    if (NIL_P(scheduler))
        rb_raise(rb_eRuntimeError, "BoundedQueue wait requires an active Fiber scheduler");
    VALUE fiber = rb_fiber_current();
    bq_waiter_push(list, len, cap, fiber, scheduler);

    bq_park_ctx_t ctx = {self, scheduler, fiber, q, list, len, 0};
    rb_ensure(bq_park_body, (VALUE)&ctx, bq_park_ensure, (VALUE)&ctx);
}

static VALUE bq_initialize(VALUE self, VALUE limit_value) {
    bq_t *q = bq_get(self);
    long limit = NUM2LONG(limit_value);
    if (limit < 1)
        rb_raise(rb_eArgError, "limit must be an integer >= 1");

    q->limit = (size_t)limit;
    q->capacity = bq_next_pow2(q->limit);
    q->mask = q->capacity - 1;
    q->items = ZALLOC_N(VALUE, q->capacity);
    for (size_t i = 0; i < q->capacity; i++)
        q->items[i] = Qnil;
    q->head = q->length = 0;
    q->closed = 0;
    q->close_error = Qnil;
    return self;
}

static VALUE bq_enqueue(VALUE self, VALUE item) {
    bq_t *q = bq_get(self);
    while (1) {
        if (q->closed)
            bq_raise_closed(q);
        if (q->length < q->limit) {
            size_t pos = (q->head + q->length) & q->mask;
            q->items[pos] = item;
            q->length++;
            bq_wake_one(self, &q->consumers, &q->cons_len);
            return item;
        }
        bq_park(self, q, &q->producers, &q->prod_len, &q->prod_cap);
    }
    return item;
}

static VALUE bq_dequeue(VALUE self) {
    bq_t *q = bq_get(self);
    while (1) {
        if (q->length > 0) {
            VALUE item = q->items[q->head];
            q->items[q->head] = Qnil;
            q->head = (q->head + 1) & q->mask;
            q->length--;
            bq_wake_one(self, &q->producers, &q->prod_len);
            return item;
        }
        if (q->closed)
            return Qnil;
        bq_park(self, q, &q->consumers, &q->cons_len, &q->cons_cap);
    }
    return Qnil;
}

static VALUE bq_close(VALUE self, VALUE error) {
    bq_t *q = bq_get(self);
    if (q->closed)
        return Qnil;
    q->closed = 1;
    q->close_error = rb_obj_is_kind_of(error, rb_eException) ? error : Qnil;
    bq_wake_all(self, &q->consumers, &q->cons_len);
    bq_wake_all(self, &q->producers, &q->prod_len);
    return Qnil;
}

static VALUE bq_empty_p(VALUE self) {
    return bq_get(self)->length == 0 ? Qtrue : Qfalse;
}

static VALUE bq_size(VALUE self) {
    return SIZET2NUM(bq_get(self)->length);
}

static VALUE bq_waiting_producers(VALUE self) {
    return SIZET2NUM(bq_get(self)->prod_len);
}

static VALUE bq_waiting_consumers(VALUE self) {
    return SIZET2NUM(bq_get(self)->cons_len);
}

static VALUE bq_drain(VALUE self) {
    bq_t *q = bq_get(self);
    VALUE items = rb_ary_new_capa((long)q->length);
    while (q->length > 0) {
        VALUE item = q->items[q->head];
        q->items[q->head] = Qnil;
        q->head = (q->head + 1) & q->mask;
        q->length--;
        rb_ary_push(items, item);
    }
    bq_wake_all(self, &q->producers, &q->prod_len);
    return items;
}

void pp_bounded_queue_init(VALUE mPgPipeline) {
    cShutdownError = rb_const_get(mPgPipeline, rb_intern("ShutdownError"));
    id_alive_p = rb_intern("alive?");

    cBoundedQueue = rb_define_class_under(mPgPipeline, "BoundedQueue", rb_cObject);
    rb_define_alloc_func(cBoundedQueue, bq_alloc);
    rb_define_method(cBoundedQueue, "initialize", bq_initialize, 1);
    rb_define_method(cBoundedQueue, "enqueue", bq_enqueue, 1);
    rb_define_method(cBoundedQueue, "dequeue", bq_dequeue, 0);
    rb_define_method(cBoundedQueue, "close", bq_close, 1);
    rb_define_method(cBoundedQueue, "empty?", bq_empty_p, 0);
    rb_define_method(cBoundedQueue, "size", bq_size, 0);
    rb_define_method(cBoundedQueue, "waiting_producers", bq_waiting_producers, 0);
    rb_define_method(cBoundedQueue, "waiting_consumers", bq_waiting_consumers, 0);
    rb_define_method(cBoundedQueue, "drain", bq_drain, 0);
}
