#include "ruby.h"
#include "ruby/encoding.h"
#include "ruby/fiber/scheduler.h"
#include <libpq-fe.h>
#include <limits.h>
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

static VALUE mPgPipeline;
static VALUE mNative;
static VALUE cRequestState;
static VALUE cResultBase;
static VALUE cResult;
static VALUE cDriver;

static VALUE cError;
static VALUE cUnsupportedServerError;
static VALUE cPipelineAbortedError;
static VALUE cConnectionLostError;
static VALUE cNotDispatchedError;
static VALUE cIndeterminateResultError;
static VALUE cShutdownError;
static VALUE cProtocolError;
static VALUE cQueryError;

static ID id_new;
static ID id_clear;
static ID id_clear_result_bang;

static ID id_cause_result;
static ID id_value;
static ID id_format;
static ID id_type;
static ID id_strip;
static ID id_to_a;

static VALUE sym_new;
static VALUE sym_queued;
static VALUE sym_dispatched;
static VALUE sym_done;
static VALUE sym_query;
static VALUE sym_prepare;
static VALUE sym_prepared_query;
static VALUE sym_reading;
static VALUE sym_writing;
static VALUE sym_ok;
static VALUE sym_failed;
static VALUE sym_active;

static int pp_ascii_casecmp(const char *a, const char *b) {
    for (;; a++, b++) {
        unsigned char ca = (unsigned char)*a;
        unsigned char cb = (unsigned char)*b;
        if (ca >= 'A' && ca <= 'Z')
            ca = (unsigned char)(ca + 32);
        if (cb >= 'A' && cb <= 'Z')
            cb = (unsigned char)(cb + 32);
        if (ca != cb)
            return (int)ca - (int)cb;
        if (ca == 0)
            return 0;
    }
}

static VALUE pp_error_new(VALUE klass, const char *message) {
    return rb_exc_new_cstr(klass, message);
}

NORETURN(static void pp_raise_conn_error(VALUE klass, PGconn *conn, const char *prefix));
static void pp_raise_conn_error(VALUE klass, PGconn *conn, const char *prefix) {
    const char *detail = conn ? PQerrorMessage(conn) : "libpq connection is unavailable";
    VALUE message = rb_sprintf("%s: %s", prefix, detail ? detail : "unknown libpq error");
    rb_exc_raise(rb_exc_new_str(klass, message));
}

static int pp_seal_enc_index = -1;

static int pp_seal_encoding_index(void) {
    return pp_seal_enc_index < 0 ? rb_utf8_encindex() : pp_seal_enc_index;
}

static void pp_publish_seal_encoding(int enc_index) {
    if (pp_seal_enc_index < 0)
        pp_seal_enc_index = enc_index;
}

static VALUE pp_export_to_seal_encoding(VALUE string, const char *what) {
    int target = pp_seal_encoding_index();
    if (rb_enc_get_index(string) == target || rb_enc_str_asciionly_p(string))
        return string;

    VALUE exported = rb_str_export_to_enc(string, rb_enc_from_index(target));
    if (NIL_P(exported))
        rb_raise(rb_eArgError, "could not convert %s to the connection encoding", what);
    return exported;
}

static int pp_client_encoding_index(const char *name) {
    if (!name || pp_ascii_casecmp(name, "SQL_ASCII") == 0)
        return rb_ascii8bit_encindex();

    struct pp_encoding_map {
        const char *postgres;
        const char *ruby;
    };
    static const struct pp_encoding_map mappings[] = {{"UTF8", "UTF-8"},
                                                      {"LATIN1", "ISO-8859-1"},
                                                      {"LATIN2", "ISO-8859-2"},
                                                      {"LATIN3", "ISO-8859-3"},
                                                      {"LATIN4", "ISO-8859-4"},
                                                      {"LATIN5", "ISO-8859-9"},
                                                      {"LATIN6", "ISO-8859-10"},
                                                      {"LATIN7", "ISO-8859-13"},
                                                      {"LATIN8", "ISO-8859-14"},
                                                      {"LATIN9", "ISO-8859-15"},
                                                      {"LATIN10", "ISO-8859-16"},
                                                      {"WIN1250", "Windows-1250"},
                                                      {"WIN1251", "Windows-1251"},
                                                      {"WIN1252", "Windows-1252"},
                                                      {"WIN1253", "Windows-1253"},
                                                      {"WIN1254", "Windows-1254"},
                                                      {"WIN1255", "Windows-1255"},
                                                      {"WIN1256", "Windows-1256"},
                                                      {"WIN1257", "Windows-1257"},
                                                      {"WIN1258", "Windows-1258"},
                                                      {"WIN866", "IBM866"},
                                                      {"KOI8R", "KOI8-R"},
                                                      {"KOI8U", "KOI8-U"},
                                                      {"ISO_8859_5", "ISO-8859-5"},
                                                      {"ISO_8859_6", "ISO-8859-6"},
                                                      {"ISO_8859_7", "ISO-8859-7"},
                                                      {"ISO_8859_8", "ISO-8859-8"},
                                                      {"EUC_JP", "EUC-JP"},
                                                      {"EUC_KR", "EUC-KR"},
                                                      {"EUC_CN", "GB2312"},
                                                      {"EUC_TW", "EUC-TW"},
                                                      {"SJIS", "Windows-31J"},
                                                      {"SHIFT_JIS_2004", "Shift_JIS-2004"},
                                                      {"EUC_JIS_2004", "EUC-JIS-2004"},
                                                      {"BIG5", "Big5"},
                                                      {"GBK", "GBK"},
                                                      {"UHC", "CP949"},
                                                      {"JOHAB", "CP1361"},
                                                      {"GB18030", "GB18030"}};

    for (size_t index = 0; index < sizeof(mappings) / sizeof(mappings[0]); index++) {
        if (pp_ascii_casecmp(name, mappings[index].postgres) == 0) {
            int enc_index = rb_enc_find_index(mappings[index].ruby);
            return enc_index >= 0 ? enc_index : rb_ascii8bit_encindex();
        }
    }

    return rb_ascii8bit_encindex();
}

static VALUE pp_frozen_string(const char *ptr, long len, int enc_index) {
    VALUE value = rb_enc_str_new(ptr, len, rb_enc_from_index(enc_index));
    rb_obj_freeze(value);
    return value;
}

static long pp_bound_index(long index, long size, const char *name) {
    if (index < 0)
        index += size;
    if (index < 0 || index >= size) {
        rb_raise(rb_eIndexError, "%s index %ld outside 0...%ld", name, index, size);
    }
    return index;
}

static VALUE pp_hash_lookup(VALUE hash, ID id) {
    VALUE symbol = ID2SYM(id);
    VALUE value = rb_hash_lookup2(hash, symbol, Qundef);
    if (value != Qundef)
        return value;
    return rb_hash_lookup2(hash, rb_id2str(id), Qundef);
}

typedef struct {
    VALUE string;
    long length;
    int format;
    Oid type;
} pp_param_desc_t;

static void pp_reject_embedded_nul(VALUE string, const char *name) {
    const char *ptr = RSTRING_PTR(string);
    long length = RSTRING_LEN(string);
    if (memchr(ptr, '\0', (size_t)length)) {
        rb_raise(rb_eArgError, "%s contains an embedded NUL byte", name);
    }
}

typedef enum {
    PP_REQ_NEW = 0,
    PP_REQ_QUEUED = 1,
    PP_REQ_DISPATCHED = 2,
    PP_REQ_DONE = 3
} pp_request_status_t;

typedef enum {
    PP_OP_NONE = 0,
    PP_OP_QUERY = 1,
    PP_OP_PREPARE = 2,
    PP_OP_PREPARED_QUERY = 3
} pp_operation_t;

#define PP_NO_OFFSET ((size_t)-1)

/* Payloads up to this size live inside pp_request_state_t: one allocation per
 * request instead of two. The cost is that every request carries the buffer
 * whether it needs it or not, and TypedData_Make_Struct zero-fills all of it --
 * so bigger is not automatically better. Overridable at build time so the size
 * can be A/B'd against a real workload:
 *   rake compile -- --with-cflags="-DPP_INLINE_ARENA=64"
 */
#ifndef PP_INLINE_ARENA
#define PP_INLINE_ARENA 192
#endif
#if defined(__GNUC__) || defined(__clang__)
#define PP_ARENA_ALIGNED __attribute__((aligned(16)))
#elif defined(_MSC_VER)
#define PP_ARENA_ALIGNED __declspec(align(16))
#else
#define PP_ARENA_ALIGNED
#endif

typedef struct {
    unsigned int sealed : 1;
    unsigned int encoding_sensitive : 1;
    unsigned int arena_inline : 1;
    int enc_index;
    pp_operation_t operation;
    char *arena;
    size_t arena_size;
    size_t sql_off;
    size_t name_off;
    size_t values_off;
    size_t types_off;
    size_t lengths_off;
    size_t formats_off;
    size_t body_offs_off;

    const char *sql;
    const char *name;
    int count;
    Oid *types;
    const char **values;
    int *lengths;
    int *formats;
    PP_ARENA_ALIGNED char inline_arena[PP_INLINE_ARENA];
} pp_payload_t;

static void pp_payload_rebind(pp_payload_t *payload) {
    char *base = payload->arena;
    payload->sql = payload->sql_off == PP_NO_OFFSET ? NULL : base + payload->sql_off;
    payload->name = payload->name_off == PP_NO_OFFSET ? NULL : base + payload->name_off;
    payload->types =
        payload->types_off == PP_NO_OFFSET ? NULL : (Oid *)(void *)(base + payload->types_off);
    payload->lengths =
        payload->lengths_off == PP_NO_OFFSET ? NULL : (int *)(void *)(base + payload->lengths_off);
    payload->formats =
        payload->formats_off == PP_NO_OFFSET ? NULL : (int *)(void *)(base + payload->formats_off);

    if (payload->values_off == PP_NO_OFFSET) {
        payload->values = NULL;
        return;
    }
    payload->values = (const char **)(void *)(base + payload->values_off);
    const size_t *body = (const size_t *)(const void *)(base + payload->body_offs_off);
    for (int index = 0; index < payload->count; index++) {
        payload->values[index] = body[index] == PP_NO_OFFSET ? NULL : base + body[index];
    }
}

typedef struct {
    pp_request_status_t state;
    unsigned int cancelled : 1;
    unsigned int settled : 1;
    unsigned int result_seen : 1;
    unsigned int query_boundary_seen : 1;
    VALUE result;
    VALUE error;
    VALUE waiter;
    VALUE scheduler;
    pp_payload_t payload;
} pp_request_state_t;

#define PP_PAYLOAD_HEADER_BYTES offsetof(pp_payload_t, inline_arena)

static void pp_payload_reset(pp_payload_t *payload) {
    if (payload->arena && !payload->arena_inline)
        xfree(payload->arena);
    memset(payload, 0, PP_PAYLOAD_HEADER_BYTES);
}

static size_t pp_payload_footprint(const pp_payload_t *payload) {
    if (!payload->arena || payload->arena_inline)
        return 0;
    return payload->arena_size;
}

static void pp_request_state_mark(void *ptr) {
    pp_request_state_t *state = ptr;
    if (!state)
        return;
    rb_gc_mark(state->result);
    rb_gc_mark(state->error);
    rb_gc_mark(state->waiter);
    rb_gc_mark(state->scheduler);
}

static void pp_request_state_free(void *ptr) {
    pp_request_state_t *state = ptr;
    if (state)
        pp_payload_reset(&state->payload);
    xfree(ptr);
}

static size_t pp_request_state_size(const void *ptr) {
    const pp_request_state_t *state = ptr;
    if (!state)
        return 0;
    return sizeof(pp_request_state_t) + pp_payload_footprint(&state->payload);
}

static const rb_data_type_t pp_request_state_type = {
    "PgPipeline::Native::RequestState",
    {pp_request_state_mark, pp_request_state_free, pp_request_state_size, NULL, {NULL}},
    NULL,
    NULL,
    RUBY_TYPED_FREE_IMMEDIATELY};

static VALUE pp_request_state_alloc(VALUE klass) {
    pp_request_state_t *state;
    VALUE object = TypedData_Make_Struct(klass, pp_request_state_t, &pp_request_state_type, state);
    state->state = PP_REQ_NEW;
    state->result = Qnil;
    state->error = Qnil;
    state->waiter = Qnil;
    state->scheduler = Qnil;
    return object;
}

static pp_request_state_t *pp_request_state_get(VALUE object) {
    pp_request_state_t *state;
    TypedData_Get_Struct(object, pp_request_state_t, &pp_request_state_type, state);
    return state;
}

static pp_request_state_t *pp_request_state_from_request(VALUE request) {
    return pp_request_state_get(request);
}

static VALUE pp_state_symbol(pp_request_status_t state) {
    switch (state) {
    case PP_REQ_NEW:
        return sym_new;
    case PP_REQ_QUEUED:
        return sym_queued;
    case PP_REQ_DISPATCHED:
        return sym_dispatched;
    case PP_REQ_DONE:
        return sym_done;
    }
    return Qnil;
}

static pp_request_status_t pp_state_value(VALUE value) {
    if (value == sym_new)
        return PP_REQ_NEW;
    if (value == sym_queued)
        return PP_REQ_QUEUED;
    if (value == sym_dispatched)
        return PP_REQ_DISPATCHED;
    if (value == sym_done)
        return PP_REQ_DONE;
    rb_raise(cProtocolError, "invalid request state");
}

static pp_operation_t pp_operation_value(VALUE value) {
    if (value == sym_query)
        return PP_OP_QUERY;
    if (value == sym_prepare)
        return PP_OP_PREPARE;
    if (value == sym_prepared_query)
        return PP_OP_PREPARED_QUERY;
    rb_raise(cProtocolError, "unsupported request operation");
}

static VALUE pp_operation_symbol(pp_operation_t operation) {
    switch (operation) {
    case PP_OP_QUERY:
        return sym_query;
    case PP_OP_PREPARE:
        return sym_prepare;
    case PP_OP_PREPARED_QUERY:
        return sym_prepared_query;
    case PP_OP_NONE:
        break;
    }
    return Qnil;
}

static void pp_params_describe_into(VALUE values, pp_param_desc_t *descs, long count) {
    /* Snapshot items before #to_s — callers may mutate the Array. */
    VALUE inline_items[8];
    VALUE item_tmp = 0;
    VALUE *items = (count <= 8) ? inline_items : ALLOCV_N(VALUE, item_tmp, (size_t)count);

    for (long index = 0; index < count; index++) {
        if (RARRAY_LEN(values) != count) {
            ALLOCV_END(item_tmp);
            rb_raise(rb_eArgError, "params array was mutated while sealing");
        }
        items[index] = RARRAY_AREF(values, index);
    }

    for (long index = 0; index < count; index++) {
        VALUE item = items[index];
        VALUE value = item;
        VALUE format = Qundef;
        VALUE type = Qundef;

        if (RB_TYPE_P(item, T_HASH)) {
            value = pp_hash_lookup(item, id_value);
            if (value == Qundef)
                value = Qnil;
            format = pp_hash_lookup(item, id_format);
            type = pp_hash_lookup(item, id_type);
        }

        int format_number = (format == Qundef || NIL_P(format)) ? 0 : NUM2INT(format);
        if (format_number != 0 && format_number != 1) {
            ALLOCV_END(item_tmp);
            rb_raise(rb_eArgError, "parameter format must be 0 or 1");
        }

        VALUE string = Qnil;
        if (!NIL_P(value)) {
            string = RB_TYPE_P(value, T_STRING) ? value : rb_obj_as_string(value);
            if (format_number == 0) {
                string = pp_export_to_seal_encoding(string, "text parameter");
                pp_reject_embedded_nul(string, "text parameter");
            }
            if (RSTRING_LEN(string) > INT_MAX) {
                ALLOCV_END(item_tmp);
                rb_raise(rb_eArgError, "query parameter is too large");
            }
        }

        descs[index].string = string;
        descs[index].length = NIL_P(string) ? 0 : RSTRING_LEN(string);
        descs[index].format = format_number;
        descs[index].type = (type == Qundef || NIL_P(type)) ? 0 : (Oid)NUM2UINT(type);
    }

    ALLOCV_END(item_tmp);
}

#define PP_ALIGN_UP(size, align) (((size) + ((align) - 1)) & ~((size_t)((align) - 1)))

static void pp_payload_seal(pp_payload_t *payload, pp_operation_t operation, VALUE sql, VALUE name,
                            const pp_param_desc_t *descs, long count, const Oid *prepare_oids,
                            int enc_index) {
    int with_params = (operation != PP_OP_PREPARE) && count > 0;
    int with_sql = operation != PP_OP_PREPARED_QUERY;
    int encoding_sensitive = !rb_enc_str_asciionly_p(sql);

    size_t sql_size = with_sql ? (size_t)RSTRING_LEN(sql) + 1 : 0;
    size_t name_size = NIL_P(name) ? 0 : (size_t)RSTRING_LEN(name) + 1;
    if (!NIL_P(name) && !rb_enc_str_asciionly_p(name))
        encoding_sensitive = 1;

    size_t bodies = 0;
    if (with_params) {
        for (long index = 0; index < count; index++) {
            if (NIL_P(descs[index].string))
                continue;
            bodies += (size_t)descs[index].length + 1;
            if (descs[index].format == 0 && !rb_enc_str_asciionly_p(descs[index].string))
                encoding_sensitive = 1;
        }
    }

    size_t offset = 0;
    size_t values_at = offset;
    offset += (with_params ? (size_t)count : 0) * sizeof(const char *);
    size_t body_offs_at = offset;
    offset += (with_params ? (size_t)count : 0) * sizeof(size_t);
    offset = PP_ALIGN_UP(offset, sizeof(Oid));
    size_t types_at = offset;
    offset += (size_t)count * sizeof(Oid);
    size_t lengths_at = offset;
    offset += (with_params ? (size_t)count : 0) * sizeof(int);
    size_t formats_at = offset;
    offset += (with_params ? (size_t)count : 0) * sizeof(int);
    size_t text_at = offset;
    size_t total = offset + sql_size + name_size + bodies;
    if (total < offset)
        rb_raise(rb_eArgError, "request payload is too large to seal");

    char *arena;
    if (total <= PP_INLINE_ARENA) {
        arena = payload->inline_arena;
        payload->arena_inline = 1;
    } else {
        arena = ALLOC_N(char, total ? total : 1);
        payload->arena_inline = 0;
    }
    char *cursor = arena + text_at;

    payload->arena = arena;
    payload->arena_size = total;
    payload->operation = operation;
    payload->count = (int)count;
    payload->enc_index = enc_index;
    payload->encoding_sensitive = encoding_sensitive ? 1 : 0;

    payload->values_off = with_params ? values_at : PP_NO_OFFSET;
    payload->body_offs_off = with_params ? body_offs_at : PP_NO_OFFSET;
    payload->types_off = count == 0 ? PP_NO_OFFSET : types_at;
    payload->lengths_off = with_params ? lengths_at : PP_NO_OFFSET;
    payload->formats_off = with_params ? formats_at : PP_NO_OFFSET;

    if (with_sql) {
        payload->sql_off = (size_t)(cursor - arena);
        memcpy(cursor, RSTRING_PTR(sql), sql_size - 1);
        cursor[sql_size - 1] = '\0';
        cursor += sql_size;
    } else {
        payload->sql_off = PP_NO_OFFSET;
    }

    if (name_size) {
        payload->name_off = (size_t)(cursor - arena);
        memcpy(cursor, RSTRING_PTR(name), name_size - 1);
        cursor[name_size - 1] = '\0';
        cursor += name_size;
    } else {
        payload->name_off = PP_NO_OFFSET;
    }

    Oid *types = count == 0 ? NULL : (Oid *)(void *)(arena + types_at);
    size_t *body_offs = with_params ? (size_t *)(void *)(arena + body_offs_at) : NULL;
    int *lengths = with_params ? (int *)(void *)(arena + lengths_at) : NULL;
    int *formats = with_params ? (int *)(void *)(arena + formats_at) : NULL;

    if (operation == PP_OP_PREPARE) {
        for (long index = 0; index < count; index++)
            types[index] = prepare_oids ? prepare_oids[index] : 0;
    } else {
        for (long index = 0; index < count; index++) {
            formats[index] = descs[index].format;
            types[index] = descs[index].type;
            if (NIL_P(descs[index].string)) {
                body_offs[index] = PP_NO_OFFSET;
                lengths[index] = 0;
            } else {
                long length = descs[index].length;
                memcpy(cursor, RSTRING_PTR(descs[index].string), (size_t)length);
                cursor[length] = '\0';
                body_offs[index] = (size_t)(cursor - arena);
                lengths[index] = (int)length;
                cursor += length + 1;
            }
        }
    }

    pp_payload_rebind(payload);
    payload->sealed = 1;
}

static VALUE pp_request_state_seal(VALUE self, VALUE operation_value, VALUE sql, VALUE params,
                                   VALUE name, VALUE param_types) {
    pp_request_state_t *state = pp_request_state_get(self);
    if (state->payload.sealed)
        rb_raise(cProtocolError, "request payload is already sealed");
    if (state->state != PP_REQ_NEW)
        rb_raise(cProtocolError, "request must be new when sealed");

    pp_operation_t operation = pp_operation_value(operation_value);
    int enc_index = pp_seal_encoding_index();

    StringValue(sql);
    sql = pp_export_to_seal_encoding(sql, "SQL");
    pp_reject_embedded_nul(sql, "SQL");

    if (operation == PP_OP_QUERY) {
        if (!NIL_P(name))
            rb_raise(cProtocolError, "plain query must not carry a statement name");
    } else {
        if (NIL_P(name))
            rb_raise(cProtocolError, "prepared operations require a statement name");
        StringValue(name);
        name = pp_export_to_seal_encoding(name, "prepared statement name");
        pp_reject_embedded_nul(name, "prepared statement name");
        if (RSTRING_LEN(name) == 0)
            rb_raise(rb_eArgError, "statement_name must not be empty");
    }

    pp_param_desc_t inline_descs[8];
    pp_param_desc_t *descs = NULL;
    VALUE desc_tmp = 0;
    Oid inline_oids[8];
    Oid *prepare_oids = NULL;
    VALUE oid_tmp = 0;
    long count = 0;

    if (operation == PP_OP_PREPARE) {
        if (!NIL_P(param_types)) {
            Check_Type(param_types, T_ARRAY);
            count = RARRAY_LEN(param_types);
            if (count > INT_MAX)
                rb_raise(rb_eArgError, "too many prepared parameter types");
            if (count > 0) {
                prepare_oids = (count <= 8) ? inline_oids : ALLOCV_N(Oid, oid_tmp, (size_t)count);
                for (long index = 0; index < count; index++) {
                    VALUE oid = RARRAY_AREF(param_types, index);
                    prepare_oids[index] = NIL_P(oid) ? 0 : (Oid)NUM2UINT(oid);
                }
            }
        }
    } else if (!NIL_P(params)) {
        Check_Type(params, T_ARRAY);
        count = RARRAY_LEN(params);
        if (count > INT_MAX)
            rb_raise(rb_eArgError, "too many query parameters");
        if (count > 0) {
            descs =
                (count <= 8) ? inline_descs : ALLOCV_N(pp_param_desc_t, desc_tmp, (size_t)count);
            pp_params_describe_into(params, descs, count);
        }
    }

    pp_payload_seal(&state->payload, operation, sql, name, descs, count, prepare_oids, enc_index);

    if (desc_tmp)
        ALLOCV_END(desc_tmp);
    if (oid_tmp)
        ALLOCV_END(oid_tmp);
    RB_GC_GUARD(sql);
    RB_GC_GUARD(name);
    RB_GC_GUARD(params);
    RB_GC_GUARD(param_types);
    return self;
}

static VALUE pp_request_state_adopt_payload(VALUE self, VALUE source) {
    pp_request_state_t *state = pp_request_state_get(self);
    pp_request_state_t *origin = pp_request_state_get(source);

    if (state->payload.sealed)
        rb_raise(cProtocolError, "request payload is already sealed");
    if (!origin->payload.sealed)
        rb_raise(cProtocolError, "source request payload is not sealed");
    if (state->state != PP_REQ_NEW)
        rb_raise(cProtocolError, "request must be new when sealed");

    pp_payload_t *from = &origin->payload;
    pp_payload_t *to = &state->payload;
    size_t bytes = from->arena_size ? from->arena_size : 1;

    memcpy(to, from, PP_PAYLOAD_HEADER_BYTES);
    if (from->arena_inline && from->arena_size <= PP_INLINE_ARENA) {
        memcpy(to->inline_arena, from->inline_arena, from->arena_size);
        to->arena = to->inline_arena;
        to->arena_inline = 1;
    } else {
        char *arena = ALLOC_N(char, bytes);
        memcpy(arena, from->arena, from->arena_size);
        to->arena = arena;
        to->arena_inline = 0;
    }
    pp_payload_rebind(to);
    to->sealed = 1;
    return self;
}

static VALUE pp_request_state_sealed_p(VALUE self) {
    return pp_request_state_get(self)->payload.sealed ? Qtrue : Qfalse;
}

static VALUE pp_request_state_payload_digest(VALUE self) {
    pp_request_state_t *state = pp_request_state_get(self);
    pp_payload_t *payload = &state->payload;
    if (!payload->sealed)
        return Qnil;

    VALUE hash = rb_hash_new();
    rb_hash_aset(hash, ID2SYM(rb_intern("operation")), pp_operation_symbol(payload->operation));
    VALUE sql;
    if (payload->sql) {
        sql = rb_str_new_cstr(payload->sql);
    } else {
        sql = rb_iv_get(self, "@sql");
        StringValue(sql);
        if (rb_enc_get_index(sql) != payload->enc_index && !rb_enc_str_asciionly_p(sql))
            sql = rb_str_export_to_enc(sql, rb_enc_from_index(payload->enc_index));
        sql = rb_str_new(RSTRING_PTR(sql), RSTRING_LEN(sql));
    }
    rb_hash_aset(hash, ID2SYM(rb_intern("sql")), sql);
    rb_hash_aset(hash, ID2SYM(rb_intern("statement_name")),
                 payload->name ? rb_str_new_cstr(payload->name) : Qnil);
    rb_hash_aset(hash, ID2SYM(rb_intern("count")), INT2NUM(payload->count));
    rb_hash_aset(hash, ID2SYM(rb_intern("bytes")), SIZET2NUM(payload->arena_size));
    rb_hash_aset(hash, ID2SYM(rb_intern("encoding")),
                 rb_enc_from_encoding(rb_enc_from_index(payload->enc_index)));
    rb_hash_aset(hash, ID2SYM(rb_intern("encoding_sensitive")),
                 payload->encoding_sensitive ? Qtrue : Qfalse);

    VALUE values = rb_ary_new_capa(payload->values ? payload->count : 0);
    VALUE formats = rb_ary_new_capa(payload->formats ? payload->count : 0);
    VALUE types = rb_ary_new_capa(payload->types ? payload->count : 0);
    for (int index = 0; index < payload->count; index++) {
        if (payload->values) {
            rb_ary_push(values, payload->values[index]
                                    ? rb_str_new(payload->values[index], payload->lengths[index])
                                    : Qnil);
        }
        if (payload->formats)
            rb_ary_push(formats, INT2NUM(payload->formats[index]));
        if (payload->types)
            rb_ary_push(types, UINT2NUM(payload->types[index]));
    }
    rb_hash_aset(hash, ID2SYM(rb_intern("values")), values);
    rb_hash_aset(hash, ID2SYM(rb_intern("formats")), formats);
    rb_hash_aset(hash, ID2SYM(rb_intern("types")), types);
    return hash;
}

static void pp_result_clear(VALUE result) {
    if (!NIL_P(result) && rb_respond_to(result, id_clear)) {
        rb_funcall(result, id_clear, 0);
    }
}

static void pp_request_wake(VALUE request, pp_request_state_t *state) {
    VALUE waiter = state->waiter;
    VALUE scheduler = state->scheduler;
    state->waiter = Qnil;
    state->scheduler = Qnil;

    if (!NIL_P(waiter) && !NIL_P(scheduler)) {
        rb_fiber_scheduler_unblock(scheduler, request, waiter);
    }
}

static void pp_request_transition(pp_request_state_t *state, pp_request_status_t from,
                                  pp_request_status_t to) {
    if (state->state != from) {
        rb_raise(cProtocolError, "invalid request transition %" PRIsVALUE " -> %" PRIsVALUE,
                 pp_state_symbol(state->state), pp_state_symbol(to));
    }
    state->state = to;
}

static void pp_request_accept_result(pp_request_state_t *state, VALUE result) {
    if (state->query_boundary_seen)
        rb_raise(cProtocolError, "result arrived after query boundary");
    if (state->result_seen)
        rb_raise(cProtocolError, "multiple results for one pipeline unit");
    state->result_seen = 1;

    if (state->cancelled) {
        pp_result_clear(result);
    } else {
        state->result = result;
    }
}

static void pp_request_record_error(pp_request_state_t *state, VALUE error, VALUE result) {
    if (state->query_boundary_seen)
        rb_raise(cProtocolError, "result arrived after query boundary");
    if (state->result_seen)
        rb_raise(cProtocolError, "multiple results for one pipeline unit");
    state->result_seen = 1;

    if (state->cancelled) {
        pp_result_clear(result);
    } else if (NIL_P(state->error)) {
        state->error = error;
    }
}

static void pp_request_query_boundary(pp_request_state_t *state) {
    if (!state->result_seen)
        rb_raise(cProtocolError, "query boundary before query result");
    if (state->query_boundary_seen)
        rb_raise(cProtocolError, "duplicate query boundary");
    state->query_boundary_seen = 1;
}

static void pp_request_finish(VALUE request, pp_request_state_t *state) {
    if (!state->query_boundary_seen)
        rb_raise(cProtocolError, "sync arrived before query boundary");
    if (state->settled)
        return;

    state->settled = 1;
    state->state = PP_REQ_DONE;
    if (!state->cancelled)
        pp_request_wake(request, state);
}

static void pp_request_reject(VALUE request, pp_request_state_t *state, VALUE error) {
    if (state->settled)
        return;

    pp_result_clear(state->result);
    state->result = Qnil;
    if (NIL_P(state->error))
        state->error = error;
    state->settled = 1;
    state->state = PP_REQ_DONE;
    if (!state->cancelled)
        pp_request_wake(request, state);
}

static VALUE pp_request_state_initialize(VALUE self) {
    return self;
}

static VALUE pp_request_state_state(VALUE self) {
    return pp_state_symbol(pp_request_state_get(self)->state);
}

static VALUE pp_request_state_set_state(VALUE self, VALUE value) {
    pp_request_state_get(self)->state = pp_state_value(value);
    return value;
}

#define PP_BOOL_GETTER(name, field)                                \
    static VALUE pp_request_state_##name(VALUE self) {             \
        return pp_request_state_get(self)->field ? Qtrue : Qfalse; \
    }

#define PP_BOOL_SETTER(name, field)                                     \
    static VALUE pp_request_state_set_##name(VALUE self, VALUE value) { \
        pp_request_state_get(self)->field = RTEST(value);               \
        return value;                                                   \
    }

PP_BOOL_GETTER(cancelled, cancelled)
PP_BOOL_SETTER(cancelled, cancelled)
PP_BOOL_GETTER(settled, settled)
PP_BOOL_SETTER(settled, settled)
PP_BOOL_GETTER(result_seen, result_seen)
PP_BOOL_SETTER(result_seen, result_seen)
PP_BOOL_GETTER(query_boundary_seen, query_boundary_seen)
PP_BOOL_SETTER(query_boundary_seen, query_boundary_seen)

#define PP_VALUE_GETTER(name, field)                   \
    static VALUE pp_request_state_##name(VALUE self) { \
        return pp_request_state_get(self)->field;      \
    }
#define PP_VALUE_SETTER(name, field)                                    \
    static VALUE pp_request_state_set_##name(VALUE self, VALUE value) { \
        pp_request_state_get(self)->field = value;                      \
        return value;                                                   \
    }

PP_VALUE_GETTER(result, result)
PP_VALUE_SETTER(result, result)
PP_VALUE_GETTER(error, error)
PP_VALUE_SETTER(error, error)
PP_VALUE_GETTER(waiter, waiter)
PP_VALUE_SETTER(waiter, waiter)
PP_VALUE_GETTER(waiter_scheduler, scheduler)
PP_VALUE_SETTER(waiter_scheduler, scheduler)

static VALUE pp_request_state_transition(VALUE self, VALUE from, VALUE to) {
    pp_request_transition(pp_request_state_get(self), pp_state_value(from), pp_state_value(to));
    return self;
}

static VALUE pp_request_state_accept_result(VALUE self, VALUE result) {
    pp_request_accept_result(pp_request_state_get(self), result);
    return result;
}

static VALUE pp_request_state_record_error(VALUE self, VALUE error, VALUE result) {
    pp_request_record_error(pp_request_state_get(self), error, result);
    return error;
}

static VALUE pp_request_state_query_boundary(VALUE self) {
    pp_request_query_boundary(pp_request_state_get(self));
    return self;
}

static VALUE pp_request_state_finish(VALUE self, VALUE request) {
    pp_request_finish(request, pp_request_state_get(self));
    return self;
}

static VALUE pp_request_state_reject(VALUE self, VALUE request, VALUE error) {
    pp_request_reject(request, pp_request_state_get(self), error);
    return self;
}

static VALUE pp_request_state_cancel(VALUE self) {
    pp_request_state_t *state = pp_request_state_get(self);
    if (state->cancelled)
        return self;

    state->cancelled = 1;
    pp_result_clear(state->result);
    state->result = Qnil;
    if (!NIL_P(state->error) && rb_respond_to(state->error, id_clear_result_bang)) {
        rb_funcall(state->error, id_clear_result_bang, 0);
    }
    return self;
}

typedef struct {
    VALUE self;
    VALUE request;
    VALUE scheduler;
} pp_wait_ctx_t;

static VALUE pp_request_wait_loop(VALUE arg) {
    pp_wait_ctx_t *ctx = (pp_wait_ctx_t *)arg;
    pp_request_state_t *state = pp_request_state_get(ctx->self);

    while (!state->settled) {
        rb_fiber_scheduler_block(ctx->scheduler, ctx->request, Qnil);
    }
    return Qnil;
}

static VALUE pp_request_state_wait(VALUE self, VALUE request) {
    pp_request_state_t *state = pp_request_state_get(self);

    if (!state->settled) {
        if (!NIL_P(state->waiter))
            rb_raise(cProtocolError, "request already has a waiter");

        VALUE scheduler = rb_fiber_scheduler_current();
        if (NIL_P(scheduler))
            rb_raise(cError, "request wait requires an active Fiber scheduler");
        VALUE waiter = rb_fiber_current();

        state->waiter = waiter;
        state->scheduler = scheduler;

        pp_wait_ctx_t ctx = {self, request, scheduler};
        int raised = 0;
        rb_protect(pp_request_wait_loop, (VALUE)&ctx, &raised);

        state = pp_request_state_get(self);
        if (state->waiter == waiter) {
            state->waiter = Qnil;
            state->scheduler = Qnil;
        }
        if (raised)
            rb_jump_tag(raised);
    }

    if (!NIL_P(state->error))
        rb_exc_raise(state->error);
    return state->result;
}

typedef struct {
    PGresult *result;
    VALUE fields_cache;
    int enc_index;
    size_t external_bytes;
} pp_result_t;

static void pp_result_release(pp_result_t *result, int adjust_gc) {
    if (!result->result)
        return;
    PQclear(result->result);
    result->result = NULL;
    if (result->external_bytes) {
        if (adjust_gc)
            rb_gc_adjust_memory_usage(-(ssize_t)result->external_bytes);
        result->external_bytes = 0;
    }
}

static void pp_result_mark(void *ptr) {
    pp_result_t *result = ptr;
    if (result)
        rb_gc_mark(result->fields_cache);
}

static void pp_result_free(void *ptr) {
    pp_result_t *result = ptr;
    if (!result)
        return;
    pp_result_release(result, 0);
    xfree(result);
}

static size_t pp_result_size(const void *ptr) {
    const pp_result_t *result = ptr;
    return result ? sizeof(pp_result_t) + result->external_bytes : 0;
}

static const rb_data_type_t pp_result_type = {
    "PgPipeline::Native::Result",
    {pp_result_mark, pp_result_free, pp_result_size, NULL, {NULL}},
    NULL,
    NULL,
    RUBY_TYPED_FREE_IMMEDIATELY};

static VALUE pp_result_alloc(VALUE klass) {
    pp_result_t *result;
    VALUE object = TypedData_Make_Struct(klass, pp_result_t, &pp_result_type, result);
    result->result = NULL;
    result->fields_cache = Qnil;
    result->enc_index = rb_utf8_encindex();
    result->external_bytes = 0;
    return object;
}

static pp_result_t *pp_result_get(VALUE self) {
    pp_result_t *result;
    TypedData_Get_Struct(self, pp_result_t, &pp_result_type, result);
    if (!result->result)
        rb_raise(cProtocolError, "result has been cleared");
    return result;
}

static VALUE pp_result_wrap(PGresult *result, int enc_index) {
    pp_result_t *wrapped;
    VALUE object = TypedData_Make_Struct(cResult, pp_result_t, &pp_result_type, wrapped);
    wrapped->result = result;
    wrapped->fields_cache = Qnil;
    wrapped->enc_index = enc_index;
    wrapped->external_bytes = PQresultMemorySize(result);
    if (wrapped->external_bytes)
        rb_gc_adjust_memory_usage((ssize_t)wrapped->external_bytes);
    return object;
}

static VALUE pp_result_clear_method(VALUE self) {
    pp_result_t *result;
    TypedData_Get_Struct(self, pp_result_t, &pp_result_type, result);
    pp_result_release(result, 1);
    result->fields_cache = Qnil;
    return Qnil;
}

static VALUE pp_result_external_bytes(VALUE self) {
    pp_result_t *result;
    TypedData_Get_Struct(self, pp_result_t, &pp_result_type, result);
    return SIZET2NUM(result->external_bytes);
}

static VALUE pp_result_cleared_p(VALUE self) {
    pp_result_t *result;
    TypedData_Get_Struct(self, pp_result_t, &pp_result_type, result);
    return result->result ? Qfalse : Qtrue;
}

static VALUE pp_result_status(VALUE self) {
    return INT2NUM(PQresultStatus(pp_result_get(self)->result));
}

static VALUE pp_result_error_message(VALUE self) {
    pp_result_t *result = pp_result_get(self);
    const char *message = PQresultErrorMessage(result->result);
    if (!message)
        return rb_str_new_cstr("");
    return rb_enc_str_new_cstr(message, rb_enc_from_index(result->enc_index));
}

static VALUE pp_result_error_field(VALUE self, VALUE code) {
    int field_code = NUM2INT(code);
    pp_result_t *result = pp_result_get(self);
    const char *value = PQresultErrorField(result->result, field_code);
    if (!value)
        return Qnil;
    return rb_enc_str_new_cstr(value, rb_enc_from_index(result->enc_index));
}

static VALUE pp_result_ntuples(VALUE self) {
    return INT2NUM(PQntuples(pp_result_get(self)->result));
}

static VALUE pp_result_nfields(VALUE self) {
    return INT2NUM(PQnfields(pp_result_get(self)->result));
}

static VALUE pp_result_fields(VALUE self) {
    pp_result_t *result = pp_result_get(self);
    if (!NIL_P(result->fields_cache))
        return result->fields_cache;

    int count = PQnfields(result->result);
    VALUE fields = rb_ary_new_capa(count);
    for (int index = 0; index < count; index++) {
        const char *name = PQfname(result->result, index);
        rb_ary_push(fields, pp_frozen_string(name ? name : "", name ? (long)strlen(name) : 0,
                                             result->enc_index));
    }
    rb_obj_freeze(fields);
    result->fields_cache = fields;
    return fields;
}

static VALUE pp_result_ftype(VALUE self, VALUE index_value) {
    long raw_index = NUM2LONG(index_value);
    pp_result_t *result = pp_result_get(self);
    long index = pp_bound_index(raw_index, PQnfields(result->result), "field");
    return UINT2NUM(PQftype(result->result, (int)index));
}

static VALUE pp_result_fmod(VALUE self, VALUE index_value) {
    long raw_index = NUM2LONG(index_value);
    pp_result_t *result = pp_result_get(self);
    long index = pp_bound_index(raw_index, PQnfields(result->result), "field");
    return INT2NUM(PQfmod(result->result, (int)index));
}

static VALUE pp_result_getvalue_enc(pp_result_t *result, long row, long column, rb_encoding *enc) {
    int length = PQgetlength(result->result, (int)row, (int)column);
    if (length > 0) {
        const char *value = PQgetvalue(result->result, (int)row, (int)column);
        return rb_enc_str_new(value, length, enc);
    }
    if (PQgetisnull(result->result, (int)row, (int)column))
        return Qnil;
    return rb_enc_str_new("", 0, enc);
}

static rb_encoding *pp_result_column_encoding(pp_result_t *result, int column) {
    int enc_index =
        PQfformat(result->result, column) == 1 ? rb_ascii8bit_encindex() : result->enc_index;
    return rb_enc_from_index(enc_index);
}

static void pp_result_prepare_encs(pp_result_t *result, rb_encoding **encs, int nfields) {
    rb_encoding *binary = rb_enc_from_index(rb_ascii8bit_encindex());
    rb_encoding *text = rb_enc_from_index(result->enc_index);
    for (int column = 0; column < nfields; column++) {
        encs[column] = PQfformat(result->result, column) == 1 ? binary : text;
    }
}

static VALUE pp_result_getvalue_internal(pp_result_t *result, long row, long column) {
    return pp_result_getvalue_enc(result, row, column,
                                  pp_result_column_encoding(result, (int)column));
}

static VALUE pp_result_getvalue(VALUE self, VALUE row_value, VALUE column_value) {
    long raw_row = NUM2LONG(row_value);
    long raw_column = NUM2LONG(column_value);
    pp_result_t *result = pp_result_get(self);
    long row = pp_bound_index(raw_row, PQntuples(result->result), "row");
    long column = pp_bound_index(raw_column, PQnfields(result->result), "field");
    return pp_result_getvalue_internal(result, row, column);
}

static VALUE pp_result_row_values_enc(pp_result_t *result, long row, rb_encoding **encs,
                                      int fields) {
    VALUE values = rb_ary_new_capa(fields);
    for (int column = 0; column < fields; column++) {
        rb_ary_push(values, pp_result_getvalue_enc(result, row, column, encs[column]));
    }
    return values;
}

static VALUE pp_result_row_values(pp_result_t *result, long row) {
    int fields = PQnfields(result->result);
    VALUE enc_tmp = 0;
    rb_encoding *stack_encs[16];
    rb_encoding **encs =
        fields <= 16 ? stack_encs : ALLOCV_N(rb_encoding *, enc_tmp, (size_t)fields);
    pp_result_prepare_encs(result, encs, fields);
    VALUE values = pp_result_row_values_enc(result, row, encs, fields);
    ALLOCV_END(enc_tmp);
    return values;
}

static VALUE pp_result_row_hash_enc(VALUE fields, pp_result_t *result, long row, rb_encoding **encs,
                                    int count) {
    VALUE hash = rb_hash_new_capa(count);
    for (int column = 0; column < count; column++) {
        rb_hash_aset(hash, RARRAY_AREF(fields, column),
                     pp_result_getvalue_enc(result, row, column, encs[column]));
    }
    return hash;
}

static VALUE pp_result_row_hash(VALUE self, pp_result_t *result, long row) {
    VALUE fields = pp_result_fields(self);
    int count = PQnfields(result->result);
    VALUE enc_tmp = 0;
    rb_encoding *stack_encs[16];
    rb_encoding **encs = count <= 16 ? stack_encs : ALLOCV_N(rb_encoding *, enc_tmp, (size_t)count);
    pp_result_prepare_encs(result, encs, count);
    VALUE hash = pp_result_row_hash_enc(fields, result, row, encs, count);
    ALLOCV_END(enc_tmp);
    return hash;
}

static VALUE pp_result_aref(VALUE self, VALUE row_value) {
    long raw_row = NUM2LONG(row_value);
    pp_result_t *result = pp_result_get(self);
    long row = pp_bound_index(raw_row, PQntuples(result->result), "row");
    return pp_result_row_hash(self, result, row);
}

static VALUE pp_result_first(int argc, VALUE *argv, VALUE self) {
    if (argc != 0 && argc != 1)
        rb_error_arity(argc, 0, 1);
    long requested = argc == 1 ? NUM2LONG(argv[0]) : 0;
    if (argc == 1 && requested < 0)
        rb_raise(rb_eArgError, "attempt to take negative size");

    pp_result_t *result = pp_result_get(self);
    int rows = PQntuples(result->result);

    if (argc == 0) {
        if (rows == 0)
            return Qnil;
        return pp_result_row_hash(self, result, 0);
    }
    long count = requested;
    if (count > rows)
        count = rows;
    VALUE fields = pp_result_fields(self);
    int nfields = PQnfields(result->result);
    VALUE enc_tmp = 0;
    rb_encoding *stack_encs[16];
    rb_encoding **encs =
        nfields <= 16 ? stack_encs : ALLOCV_N(rb_encoding *, enc_tmp, (size_t)nfields);
    pp_result_prepare_encs(result, encs, nfields);
    VALUE values = rb_ary_new_capa(count);
    for (long row = 0; row < count; row++) {
        rb_ary_push(values, pp_result_row_hash_enc(fields, result, row, encs, nfields));
    }
    ALLOCV_END(enc_tmp);
    return values;
}

static VALUE pp_result_fname(VALUE self, VALUE index_value) {
    long raw_index = NUM2LONG(index_value);
    pp_result_t *result = pp_result_get(self);
    long index = pp_bound_index(raw_index, PQnfields(result->result), "field");
    return RARRAY_AREF(pp_result_fields(self), index);
}

static VALUE pp_result_fnumber(VALUE self, VALUE name_value) {
    StringValue(name_value);
    pp_result_t *result = pp_result_get(self);
    int index = PQfnumber(result->result, StringValueCStr(name_value));
    return INT2NUM(index);
}

static VALUE pp_result_getisnull(VALUE self, VALUE row_value, VALUE column_value) {
    long raw_row = NUM2LONG(row_value);
    long raw_column = NUM2LONG(column_value);
    pp_result_t *result = pp_result_get(self);
    long row = pp_bound_index(raw_row, PQntuples(result->result), "row");
    long column = pp_bound_index(raw_column, PQnfields(result->result), "field");
    return PQgetisnull(result->result, (int)row, (int)column) ? Qtrue : Qfalse;
}

static VALUE pp_result_getlength(VALUE self, VALUE row_value, VALUE column_value) {
    long raw_row = NUM2LONG(row_value);
    long raw_column = NUM2LONG(column_value);
    pp_result_t *result = pp_result_get(self);
    long row = pp_bound_index(raw_row, PQntuples(result->result), "row");
    long column = pp_bound_index(raw_column, PQnfields(result->result), "field");
    return INT2NUM(PQgetlength(result->result, (int)row, (int)column));
}

static VALUE pp_result_tuple_values(VALUE self, VALUE row_value) {
    long raw_row = NUM2LONG(row_value);
    pp_result_t *result = pp_result_get(self);
    long row = pp_bound_index(raw_row, PQntuples(result->result), "row");
    return pp_result_row_values(result, row);
}

typedef struct {
    VALUE self;
    VALUE fields;
    VALUE enc_tmp;
    rb_encoding **encs;
    int nfields;
    int rows;
} pp_result_each_ctx_t;

static VALUE pp_result_each_body(VALUE arg) {
    pp_result_each_ctx_t *ctx = (pp_result_each_ctx_t *)arg;
    pp_result_t *result = pp_result_get(ctx->self);
    for (int row = 0; row < ctx->rows; row++) {
        rb_yield(pp_result_row_hash_enc(ctx->fields, result, row, ctx->encs, ctx->nfields));
        result = pp_result_get(ctx->self);
    }
    return ctx->self;
}

static VALUE pp_result_each_ensure(VALUE arg) {
    pp_result_each_ctx_t *ctx = (pp_result_each_ctx_t *)arg;
    ALLOCV_END(ctx->enc_tmp);
    return Qnil;
}

static VALUE pp_result_each(VALUE self) {
    RETURN_ENUMERATOR(self, 0, NULL);
    pp_result_t *result = pp_result_get(self);
    int rows = PQntuples(result->result);
    VALUE fields = pp_result_fields(self);
    int nfields = PQnfields(result->result);
    VALUE enc_tmp = 0;
    rb_encoding *stack_encs[16];
    rb_encoding **encs =
        nfields <= 16 ? stack_encs : ALLOCV_N(rb_encoding *, enc_tmp, (size_t)nfields);
    pp_result_prepare_encs(result, encs, nfields);

    pp_result_each_ctx_t ctx = {self, fields, enc_tmp, encs, nfields, rows};
    return rb_ensure(pp_result_each_body, (VALUE)&ctx, pp_result_each_ensure, (VALUE)&ctx);
}

static VALUE pp_result_each_row_body(VALUE arg) {
    pp_result_each_ctx_t *ctx = (pp_result_each_ctx_t *)arg;
    pp_result_t *result = pp_result_get(ctx->self);
    for (int row = 0; row < ctx->rows; row++) {
        rb_yield(pp_result_row_values_enc(result, row, ctx->encs, ctx->nfields));
        result = pp_result_get(ctx->self);
    }
    return ctx->self;
}

/* Result#each_row used to be a Ruby loop over tuple_values(index), and
 * pp_result_row_values re-derives the per-column encoding table on every call:
 * PQnfields plus PQfformat and rb_enc_from_index for each column, per row. On a
 * 50x8 result that is 50 setups where one is needed -- so the "fast" array path
 * was doing strictly more per-row work than to_a. Same shape as pp_result_each,
 * yielding arrays instead of hashes. */
static VALUE pp_result_each_row(VALUE self) {
    RETURN_ENUMERATOR(self, 0, NULL);
    pp_result_t *result = pp_result_get(self);
    int rows = PQntuples(result->result);
    int nfields = PQnfields(result->result);
    VALUE enc_tmp = 0;
    rb_encoding *stack_encs[16];
    rb_encoding **encs =
        nfields <= 16 ? stack_encs : ALLOCV_N(rb_encoding *, enc_tmp, (size_t)nfields);
    pp_result_prepare_encs(result, encs, nfields);

    pp_result_each_ctx_t ctx = {self, Qnil, enc_tmp, encs, nfields, rows};
    return rb_ensure(pp_result_each_row_body, (VALUE)&ctx, pp_result_each_ensure, (VALUE)&ctx);
}

static VALUE pp_result_values(VALUE self) {
    pp_result_t *result = pp_result_get(self);
    int rows = PQntuples(result->result);
    int nfields = PQnfields(result->result);
    VALUE enc_tmp = 0;
    rb_encoding *stack_encs[16];
    rb_encoding **encs =
        nfields <= 16 ? stack_encs : ALLOCV_N(rb_encoding *, enc_tmp, (size_t)nfields);
    pp_result_prepare_encs(result, encs, nfields);
    VALUE values = rb_ary_new_capa(rows);
    for (int row = 0; row < rows; row++) {
        rb_ary_push(values, pp_result_row_values_enc(result, row, encs, nfields));
    }
    ALLOCV_END(enc_tmp);
    return values;
}

static VALUE pp_result_to_a(VALUE self) {
    pp_result_t *result = pp_result_get(self);
    int rows = PQntuples(result->result);
    VALUE fields = pp_result_fields(self);
    int nfields = PQnfields(result->result);
    VALUE enc_tmp = 0;
    rb_encoding *stack_encs[16];
    rb_encoding **encs =
        nfields <= 16 ? stack_encs : ALLOCV_N(rb_encoding *, enc_tmp, (size_t)nfields);
    pp_result_prepare_encs(result, encs, nfields);
    VALUE values = rb_ary_new_capa(rows);
    for (int row = 0; row < rows; row++) {
        rb_ary_push(values, pp_result_row_hash_enc(fields, result, row, encs, nfields));
    }
    ALLOCV_END(enc_tmp);
    return values;
}

static VALUE pp_result_column_values(VALUE self, VALUE column_value) {
    long raw_column = NUM2LONG(column_value);
    pp_result_t *result = pp_result_get(self);
    long column = pp_bound_index(raw_column, PQnfields(result->result), "field");
    int rows = PQntuples(result->result);
    rb_encoding *enc = pp_result_column_encoding(result, (int)column);
    VALUE values = rb_ary_new_capa(rows);
    for (int row = 0; row < rows; row++) {
        rb_ary_push(values, pp_result_getvalue_enc(result, row, column, enc));
    }
    return values;
}

static VALUE pp_result_field_values(VALUE self, VALUE field_value) {
    StringValue(field_value);
    pp_result_t *result = pp_result_get(self);
    int column = PQfnumber(result->result, StringValueCStr(field_value));
    if (column < 0)
        rb_raise(rb_eIndexError, "unknown field %" PRIsVALUE, field_value);
    return pp_result_column_values(self, INT2NUM(column));
}

static VALUE pp_result_cmd_tuples(VALUE self) {
    const char *value = PQcmdTuples(pp_result_get(self)->result);
    if (!value || *value == '\0')
        return INT2NUM(0);
    return ULL2NUM(strtoull(value, NULL, 10));
}

static VALUE pp_result_length(VALUE self) {
    return INT2NUM(PQntuples(pp_result_get(self)->result));
}

typedef struct {
    uint64_t dispatches;
    uint64_t flush_calls;
    uint64_t flush_incomplete;
    uint64_t readable_events;
    uint64_t results_read;
    uint64_t units_completed;
    uint64_t bytes_dispatched;
} pp_driver_counters_t;

typedef struct {
    PGconn *conn;
    VALUE *inflight;
    size_t inflight_capacity;
    size_t inflight_head;
    size_t inflight_length;
    size_t inflight_peak;
    int enc_index;
    unsigned int pipeline_mode : 1;
    unsigned int closed : 1;
    unsigned int draining : 1;
    unsigned int close_pending : 1;
    pp_driver_counters_t counters;
} pp_driver_t;

static void pp_driver_mark(void *ptr) {
    pp_driver_t *driver = ptr;
    if (!driver || !driver->inflight)
        return;
    for (size_t index = 0; index < driver->inflight_length; index++) {
        size_t position = (driver->inflight_head + index) % driver->inflight_capacity;
        rb_gc_mark(driver->inflight[position]);
    }
}

static void pp_driver_free(void *ptr) {
    pp_driver_t *driver = ptr;
    if (!driver)
        return;
    if (driver->conn)
        PQfinish(driver->conn);
    xfree(driver->inflight);
    xfree(driver);
}

static size_t pp_driver_size(const void *ptr) {
    const pp_driver_t *driver = ptr;
    if (!driver)
        return 0;
    return sizeof(*driver) + sizeof(VALUE) * driver->inflight_capacity;
}

static const rb_data_type_t pp_driver_type = {
    "PgPipeline::Native::Driver",
    {pp_driver_mark, pp_driver_free, pp_driver_size, NULL, {NULL}},
    NULL,
    NULL,
    RUBY_TYPED_FREE_IMMEDIATELY};

static VALUE pp_driver_alloc(VALUE klass) {
    pp_driver_t *driver;
    VALUE object = TypedData_Make_Struct(klass, pp_driver_t, &pp_driver_type, driver);
    memset(driver, 0, sizeof(*driver));
    driver->enc_index = rb_utf8_encindex();
    return object;
}

static pp_driver_t *pp_driver_get(VALUE self) {
    pp_driver_t *driver;
    TypedData_Get_Struct(self, pp_driver_t, &pp_driver_type, driver);
    return driver;
}

static void pp_driver_ensure_conn(pp_driver_t *driver) {
    if (!driver->conn || driver->closed)
        rb_raise(cConnectionLostError, "native connection is closed");
}

static VALUE pp_stringify(VALUE value) {
    if (SYMBOL_P(value))
        return rb_sym2str(value);
    return rb_obj_as_string(value);
}

static void pp_notice_processor(void *arg, const char *message) {
    (void)arg;
    (void)message;
}

static PGconn *pp_connect_start(VALUE connection_args) {
    if (NIL_P(connection_args))
        return PQconnectStart("");

    if (RB_TYPE_P(connection_args, T_STRING)) {
        return PQconnectStart(StringValueCStr(connection_args));
    }

    if (RB_TYPE_P(connection_args, T_HASH)) {
        VALUE pairs = rb_funcall(connection_args, id_to_a, 0);
        VALUE holders = rb_ary_new();
        long pair_count = RARRAY_LEN(pairs);

        for (long index = 0; index < pair_count; index++) {
            VALUE pair = RARRAY_AREF(pairs, index);
            VALUE value = RARRAY_AREF(pair, 1);
            if (NIL_P(value))
                continue;
            rb_ary_push(holders, pp_stringify(RARRAY_AREF(pair, 0)));
            rb_ary_push(holders, pp_stringify(value));
        }

        long count = RARRAY_LEN(holders) / 2;

        for (long index = 0; index < count * 2; index++) {
            VALUE item = RARRAY_AREF(holders, index);
            (void)StringValueCStr(item);
        }

        const char **keywords = ALLOC_N(const char *, count + 1);
        const char **values = ALLOC_N(const char *, count + 1);
        for (long index = 0; index < count; index++) {
            VALUE key = RARRAY_AREF(holders, index * 2);
            VALUE value = RARRAY_AREF(holders, index * 2 + 1);
            keywords[index] = RSTRING_PTR(key);
            values[index] = RSTRING_PTR(value);
        }
        keywords[count] = NULL;
        values[count] = NULL;
        PGconn *conn = PQconnectStartParams(keywords, values, 1);
        xfree(keywords);
        xfree(values);
        RB_GC_GUARD(holders);
        return conn;
    }

    rb_raise(rb_eArgError,
             "native backend connection_args must be nil, a conninfo String, or a Hash");
}

static VALUE pp_driver_initialize(VALUE self, VALUE connection_args, VALUE max_in_flight_value) {
    pp_driver_t *driver = pp_driver_get(self);
    long max_in_flight = NUM2LONG(max_in_flight_value);
    if (max_in_flight < 1)
        rb_raise(rb_eArgError, "max_in_flight must be >= 1");

    driver->inflight_capacity = (size_t)max_in_flight;
    driver->inflight = ALLOC_N(VALUE, driver->inflight_capacity);
    for (size_t index = 0; index < driver->inflight_capacity; index++)
        driver->inflight[index] = Qnil;

    driver->conn = pp_connect_start(connection_args);
    if (!driver->conn)
        rb_raise(cConnectionLostError, "PQconnectStart failed to allocate a connection");
    PQsetNoticeProcessor(driver->conn, pp_notice_processor, NULL);
    return self;
}

static VALUE pp_driver_connect_poll(VALUE self) {
    pp_driver_t *driver = pp_driver_get(self);
    pp_driver_ensure_conn(driver);

    switch (PQconnectPoll(driver->conn)) {
    case PGRES_POLLING_READING:
        return sym_reading;
    case PGRES_POLLING_WRITING:
        return sym_writing;
    case PGRES_POLLING_OK: {
        if (PQsetnonblocking(driver->conn, 1) != 0) {
            pp_raise_conn_error(cConnectionLostError, driver->conn, "PQsetnonblocking failed");
        }
        const char *encoding = PQparameterStatus(driver->conn, "client_encoding");
        driver->enc_index = pp_client_encoding_index(encoding);
        pp_publish_seal_encoding(driver->enc_index);
        return sym_ok;
    }
    case PGRES_POLLING_ACTIVE:
        return sym_active;
    case PGRES_POLLING_FAILED:
        return sym_failed;
    }
    return sym_failed;
}

static VALUE pp_driver_encoding(VALUE self) {
    pp_driver_t *driver = pp_driver_get(self);
    return rb_enc_from_encoding(rb_enc_from_index(driver->enc_index));
}

static VALUE pp_driver_socket(VALUE self) {
    pp_driver_t *driver = pp_driver_get(self);
    pp_driver_ensure_conn(driver);
    int socket = PQsocket(driver->conn);
    if (socket < 0)
        rb_raise(cConnectionLostError, "libpq connection has no socket");
    return INT2NUM(socket);
}

static VALUE pp_driver_error_message(VALUE self) {
    pp_driver_t *driver = pp_driver_get(self);
    const char *message = driver->conn ? PQerrorMessage(driver->conn) : "connection closed";
    return rb_str_new_cstr(message ? message : "unknown libpq error");
}

static VALUE pp_driver_protocol_version(VALUE self) {
    pp_driver_t *driver = pp_driver_get(self);
    pp_driver_ensure_conn(driver);
    return INT2NUM(PQprotocolVersion(driver->conn));
}

static VALUE pp_driver_server_version(VALUE self) {
    pp_driver_t *driver = pp_driver_get(self);
    pp_driver_ensure_conn(driver);
    return INT2NUM(PQserverVersion(driver->conn));
}

static VALUE pp_driver_status(VALUE self) {
    pp_driver_t *driver = pp_driver_get(self);
    if (!driver->conn)
        return INT2NUM(CONNECTION_BAD);
    return INT2NUM(PQstatus(driver->conn));
}

static VALUE pp_driver_pipeline_status(VALUE self) {
    pp_driver_t *driver = pp_driver_get(self);
    pp_driver_ensure_conn(driver);
    return INT2NUM(PQpipelineStatus(driver->conn));
}

static VALUE pp_driver_transaction_status(VALUE self) {
    pp_driver_t *driver = pp_driver_get(self);
    pp_driver_ensure_conn(driver);
    return INT2NUM(PQtransactionStatus(driver->conn));
}

static VALUE pp_driver_enter_pipeline(VALUE self) {
    pp_driver_t *driver = pp_driver_get(self);
    pp_driver_ensure_conn(driver);
    if (PQlibVersion() < 140000 || PQprotocolVersion(driver->conn) != 3) {
        rb_raise(
            cUnsupportedServerError,
            "native backend requires libpq >= 14 and PostgreSQL protocol v3; libpq=%d protocol=%d",
            PQlibVersion(), PQprotocolVersion(driver->conn));
    }
    if (!PQenterPipelineMode(driver->conn))
        pp_raise_conn_error(cConnectionLostError, driver->conn, "PQenterPipelineMode failed");
    driver->pipeline_mode = 1;
    return Qtrue;
}

static VALUE pp_driver_exit_pipeline(VALUE self) {
    pp_driver_t *driver = pp_driver_get(self);
    pp_driver_ensure_conn(driver);
    if (!driver->pipeline_mode)
        return Qtrue;
    if (!PQexitPipelineMode(driver->conn))
        pp_raise_conn_error(cConnectionLostError, driver->conn, "PQexitPipelineMode failed");
    driver->pipeline_mode = 0;
    return Qtrue;
}

static void pp_driver_finish_conn(pp_driver_t *driver) {
    if (driver->conn) {
        PQfinish(driver->conn);
        driver->conn = NULL;
    }
}

static VALUE pp_driver_close(VALUE self) {
    pp_driver_t *driver = pp_driver_get(self);
    driver->closed = 1;
    if (driver->draining) {
        driver->close_pending = 1;
        return Qnil;
    }
    pp_driver_finish_conn(driver);
    return Qnil;
}

static VALUE pp_driver_closed_p(VALUE self) {
    pp_driver_t *driver = pp_driver_get(self);
    return (!driver->conn || driver->closed) ? Qtrue : Qfalse;
}

static VALUE pp_driver_inflight_count(VALUE self) {
    return SIZET2NUM(pp_driver_get(self)->inflight_length);
}

static VALUE pp_driver_inflight_plus(VALUE self, VALUE extra) {
    size_t base = pp_driver_get(self)->inflight_length;
    long add = NUM2LONG(extra);
    if (add < 0)
        rb_raise(rb_eArgError, "extra load must be non-negative");
    return SIZET2NUM(base + (size_t)add);
}

static VALUE pp_driver_reusable_p(VALUE self) {
    pp_driver_t *driver = pp_driver_get(self);
    if (!driver->conn || driver->closed)
        return Qfalse;
    return (PQstatus(driver->conn) == CONNECTION_OK &&
            PQpipelineStatus(driver->conn) != PQ_PIPELINE_OFF)
               ? Qtrue
               : Qfalse;
}

static VALUE pp_driver_pipeline_aborted_p(VALUE self) {
    pp_driver_t *driver = pp_driver_get(self);
    if (!driver->conn || driver->closed)
        return Qfalse;
    return PQpipelineStatus(driver->conn) == PQ_PIPELINE_ABORTED ? Qtrue : Qfalse;
}

static void pp_driver_push(pp_driver_t *driver, VALUE request) {
    if (driver->inflight_length >= driver->inflight_capacity)
        rb_raise(cProtocolError, "native in-flight queue is full");
    size_t position = (driver->inflight_head + driver->inflight_length) % driver->inflight_capacity;
    driver->inflight[position] = request;
    driver->inflight_length++;
    if (driver->inflight_length > driver->inflight_peak)
        driver->inflight_peak = driver->inflight_length;
}

static VALUE pp_driver_front(pp_driver_t *driver) {
    if (driver->inflight_length == 0)
        return Qnil;
    return driver->inflight[driver->inflight_head];
}

static VALUE pp_driver_pop(pp_driver_t *driver) {
    if (driver->inflight_length == 0)
        return Qnil;
    VALUE request = driver->inflight[driver->inflight_head];
    driver->inflight[driver->inflight_head] = Qnil;
    driver->inflight_head = (driver->inflight_head + 1) % driver->inflight_capacity;
    driver->inflight_length--;
    return request;
}

static int pp_send_query(pp_driver_t *driver, pp_request_state_t *state) {
    pp_payload_t *payload = &state->payload;
    if (!payload->sealed)
        rb_raise(cProtocolError, "request payload was not sealed before dispatch");

    switch (payload->operation) {
    case PP_OP_QUERY:
        return PQsendQueryParams(driver->conn, payload->sql, payload->count, payload->types,
                                 payload->values, payload->lengths, payload->formats, 0);
    case PP_OP_PREPARE:
        return PQsendPrepare(driver->conn, payload->name, payload->sql, payload->count,
                             payload->types);
    case PP_OP_PREPARED_QUERY:
        return PQsendQueryPrepared(driver->conn, payload->name, payload->count, payload->values,
                                   payload->lengths, payload->formats, 0);
    case PP_OP_NONE:
        break;
    }

    rb_raise(cProtocolError, "unsupported request operation");
}

static int pp_pipeline_sync(PGconn *conn) {
#ifdef HAVE_PQSENDPIPELINESYNC
    if (PQlibVersion() >= 170000)
        return PQsendPipelineSync(conn);
#endif
    return PQpipelineSync(conn);
}

static VALUE pp_driver_dispatch(VALUE self, VALUE request) {
    pp_driver_t *driver = pp_driver_get(self);
    pp_driver_ensure_conn(driver);
    if (!driver->pipeline_mode)
        rb_raise(cProtocolError, "native driver is not in pipeline mode");
    if (driver->inflight_length >= driver->inflight_capacity)
        rb_raise(cProtocolError, "native in-flight queue is full");

    pp_request_state_t *state = pp_request_state_from_request(request);
    if (state->state != PP_REQ_QUEUED)
        rb_raise(cProtocolError, "request must be queued before dispatch");

    if (state->payload.encoding_sensitive && state->payload.enc_index != driver->enc_index) {
        rb_raise(cUnsupportedServerError,
                 "request was sealed for client_encoding %s but this connection negotiated %s; "
                 "all multiplexed connections in one process must share a client_encoding",
                 rb_enc_name(rb_enc_from_index(state->payload.enc_index)),
                 rb_enc_name(rb_enc_from_index(driver->enc_index)));
    }

    if (!pp_send_query(driver, state)) {
        pp_raise_conn_error(cNotDispatchedError, driver->conn,
                            "libpq rejected query before dispatch");
    }

    if (!pp_pipeline_sync(driver->conn)) {
        pp_raise_conn_error(cConnectionLostError, driver->conn,
                            "query was accepted but pipeline Sync failed");
    }

    pp_request_transition(state, PP_REQ_QUEUED, PP_REQ_DISPATCHED);
    pp_driver_push(driver, request);
    driver->counters.dispatches++;
    driver->counters.bytes_dispatched += state->payload.arena_size;
    return Qtrue;
}

static VALUE pp_driver_flush(VALUE self) {
    pp_driver_t *driver = pp_driver_get(self);
    pp_driver_ensure_conn(driver);
    driver->counters.flush_calls++;
    int status = PQflush(driver->conn);
    if (status < 0)
        pp_raise_conn_error(cConnectionLostError, driver->conn, "flush failed");
    if (status != 0) {
        driver->counters.flush_incomplete++;
        return Qfalse;
    }
    return Qtrue;
}

static VALUE pp_query_error(PGresult *result, int enc_index) {
    VALUE wrapped = pp_result_wrap(result, enc_index);
    VALUE message = rb_funcall(pp_result_error_message(wrapped), id_strip, 0);
    if (RSTRING_LEN(message) == 0)
        message = rb_str_new_cstr("query failed");
    VALUE kwargs = rb_hash_new();
    rb_hash_aset(kwargs, ID2SYM(id_cause_result), wrapped);
    VALUE argv[2] = {message, kwargs};
    return rb_class_new_instance_kw(2, argv, cQueryError, RB_PASS_KEYWORDS);
}

static int pp_success_status(ExecStatusType status) {
    return status == PGRES_EMPTY_QUERY || status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK;
}

static int pp_copy_status(ExecStatusType status) {
    return status == PGRES_COPY_IN || status == PGRES_COPY_OUT || status == PGRES_COPY_BOTH;
}

typedef struct {
    int units_completed;
    int results_read;
} pp_drain_stats_t;

typedef struct {
    pp_driver_t *driver;
    pp_drain_stats_t stats;
} pp_drain_ctx_t;

static VALUE pp_driver_drain_body(VALUE arg) {
    pp_drain_ctx_t *ctx = (pp_drain_ctx_t *)arg;
    pp_driver_t *driver = ctx->driver;

    while (driver->inflight_length > 0) {
        pp_driver_ensure_conn(driver);
        if (PQisBusy(driver->conn))
            break;

        PGresult *result = PQgetResult(driver->conn);
        ctx->stats.results_read++;
        driver->counters.results_read++;
        VALUE request = pp_driver_front(driver);
        if (NIL_P(request)) {
            if (result)
                PQclear(result);
            rb_raise(cProtocolError, "result without an in-flight request");
        }
        pp_request_state_t *state = pp_request_state_from_request(request);

        if (!result) {
            pp_request_query_boundary(state);
            continue;
        }

        ExecStatusType status = PQresultStatus(result);
        if (status == PGRES_PIPELINE_SYNC) {
            PQclear(result);
            if (!state->query_boundary_seen)
                rb_raise(cProtocolError, "sync arrived before query boundary");

            VALUE popped = pp_driver_pop(driver);
            if (popped != request)
                rb_raise(cProtocolError, "pipeline Sync does not match FIFO front");
            ctx->stats.units_completed++;
            driver->counters.units_completed++;
            pp_request_finish(request, state);
            continue;
        }

        if (state->query_boundary_seen) {
            PQclear(result);
            rb_raise(cProtocolError, "result status %d arrived after query boundary", (int)status);
        }

        if (status == PGRES_PIPELINE_ABORTED) {
            PQclear(result);
            VALUE error = pp_error_new(cPipelineAbortedError, "pipeline unit aborted");
            pp_request_record_error(state, error, Qnil);
        } else if (status == PGRES_BAD_RESPONSE) {
            PQclear(result);
            rb_raise(cProtocolError, "server response was not understood");
        } else if (status == PGRES_FATAL_ERROR) {
            VALUE error = pp_query_error(result, driver->enc_index);
            VALUE cause = rb_funcall(error, id_cause_result, 0);
            pp_request_record_error(state, error, cause);
        } else if (pp_copy_status(status)) {
            PQclear(result);
            rb_raise(cProtocolError, "COPY is not supported on the multiplexed pipeline");
        } else if (pp_success_status(status)) {
            VALUE wrapped = pp_result_wrap(result, driver->enc_index);
            pp_request_accept_result(state, wrapped);
        } else {
            PQclear(result);
            rb_raise(cProtocolError, "unexpected pipeline result status %d", (int)status);
        }
    }

    return Qnil;
}

static VALUE pp_driver_drain_ensure(VALUE arg) {
    pp_drain_ctx_t *ctx = (pp_drain_ctx_t *)arg;
    ctx->driver->draining = 0;
    if (ctx->driver->close_pending) {
        ctx->driver->close_pending = 0;
        pp_driver_finish_conn(ctx->driver);
    }
    return Qnil;
}

static pp_drain_stats_t pp_driver_drain(pp_driver_t *driver) {
    if (driver->draining)
        rb_raise(cProtocolError, "native drain is not reentrant");

    pp_drain_ctx_t ctx;
    ctx.driver = driver;
    ctx.stats.units_completed = 0;
    ctx.stats.results_read = 0;

    driver->draining = 1;
    rb_ensure(pp_driver_drain_body, (VALUE)&ctx, pp_driver_drain_ensure, (VALUE)&ctx);
    return ctx.stats;
}

static VALUE pp_driver_consume_and_drain(VALUE self) {
    pp_driver_t *driver = pp_driver_get(self);
    pp_driver_ensure_conn(driver);
    driver->counters.readable_events++;
    if (!PQconsumeInput(driver->conn))
        pp_raise_conn_error(cConnectionLostError, driver->conn, "read failed");
    pp_drain_stats_t stats = pp_driver_drain(driver);
    return INT2NUM(stats.units_completed);
}

static VALUE pp_driver_drain_method(VALUE self) {
    pp_driver_t *driver = pp_driver_get(self);
    pp_driver_ensure_conn(driver);
    pp_drain_stats_t stats = pp_driver_drain(driver);
    return INT2NUM(stats.units_completed);
}

static VALUE pp_driver_counter(VALUE self, VALUE key) {
    pp_driver_t *driver = pp_driver_get(self);
    pp_driver_counters_t *counters = &driver->counters;
    ID id = SYMBOL_P(key) ? SYM2ID(key) : rb_intern_str(rb_String(key));

    if (id == rb_intern("dispatches"))
        return ULL2NUM(counters->dispatches);
    if (id == rb_intern("flush_calls"))
        return ULL2NUM(counters->flush_calls);
    if (id == rb_intern("flush_incomplete"))
        return ULL2NUM(counters->flush_incomplete);
    if (id == rb_intern("readable_events"))
        return ULL2NUM(counters->readable_events);
    if (id == rb_intern("results_read"))
        return ULL2NUM(counters->results_read);
    if (id == rb_intern("units_completed"))
        return ULL2NUM(counters->units_completed);
    if (id == rb_intern("bytes_dispatched"))
        return ULL2NUM(counters->bytes_dispatched);
    if (id == rb_intern("in_flight"))
        return SIZET2NUM(driver->inflight_length);
    if (id == rb_intern("in_flight_peak"))
        return SIZET2NUM(driver->inflight_peak);
    if (id == rb_intern("in_flight_capacity"))
        return SIZET2NUM(driver->inflight_capacity);

    rb_raise(rb_eKeyError, "unknown driver counter %" PRIsVALUE, key);
}

static VALUE pp_driver_stats(VALUE self) {
    pp_driver_t *driver = pp_driver_get(self);
    pp_driver_counters_t *counters = &driver->counters;
    VALUE hash = rb_hash_new();
    rb_hash_aset(hash, ID2SYM(rb_intern("dispatches")), ULL2NUM(counters->dispatches));
    rb_hash_aset(hash, ID2SYM(rb_intern("flush_calls")), ULL2NUM(counters->flush_calls));
    rb_hash_aset(hash, ID2SYM(rb_intern("flush_incomplete")), ULL2NUM(counters->flush_incomplete));
    rb_hash_aset(hash, ID2SYM(rb_intern("readable_events")), ULL2NUM(counters->readable_events));
    rb_hash_aset(hash, ID2SYM(rb_intern("results_read")), ULL2NUM(counters->results_read));
    rb_hash_aset(hash, ID2SYM(rb_intern("units_completed")), ULL2NUM(counters->units_completed));
    rb_hash_aset(hash, ID2SYM(rb_intern("bytes_dispatched")), ULL2NUM(counters->bytes_dispatched));
    rb_hash_aset(hash, ID2SYM(rb_intern("in_flight")), SIZET2NUM(driver->inflight_length));
    rb_hash_aset(hash, ID2SYM(rb_intern("in_flight_peak")), SIZET2NUM(driver->inflight_peak));
    rb_hash_aset(hash, ID2SYM(rb_intern("in_flight_capacity")),
                 SIZET2NUM(driver->inflight_capacity));
    return hash;
}

static VALUE pp_driver_front_request(VALUE self) {
    return pp_driver_front(pp_driver_get(self));
}

static VALUE pp_driver_take_inflight(VALUE self) {
    pp_driver_t *driver = pp_driver_get(self);
    VALUE requests = rb_ary_new_capa(driver->inflight_length);
    while (driver->inflight_length > 0)
        rb_ary_push(requests, pp_driver_pop(driver));
    return requests;
}

static VALUE pp_native_libpq_version(VALUE self) {
    return INT2NUM(PQlibVersion());
}

static VALUE pp_native_seal_encoding(VALUE self) {
    return rb_enc_from_encoding(rb_enc_from_index(pp_seal_encoding_index()));
}

static VALUE pp_native_seal_encoding_published_p(VALUE self) {
    return pp_seal_enc_index < 0 ? Qfalse : Qtrue;
}

static VALUE pp_native_reset_seal_encoding(VALUE self) {
    pp_seal_enc_index = -1;
    return Qnil;
}

void Init_pg_pipeline_native(void) {
    id_new = rb_intern("new");
    id_clear = rb_intern("clear");
    id_clear_result_bang = rb_intern("clear_result!");
    id_cause_result = rb_intern("cause_result");
    id_value = rb_intern("value");
    id_format = rb_intern("format");
    id_type = rb_intern("type");
    id_strip = rb_intern("strip");
    id_to_a = rb_intern("to_a");

    sym_new = ID2SYM(rb_intern("new"));
    sym_queued = ID2SYM(rb_intern("queued"));
    sym_dispatched = ID2SYM(rb_intern("dispatched"));
    sym_done = ID2SYM(rb_intern("done"));
    sym_query = ID2SYM(rb_intern("query"));
    sym_prepare = ID2SYM(rb_intern("prepare"));
    sym_prepared_query = ID2SYM(rb_intern("prepared_query"));
    sym_reading = ID2SYM(rb_intern("reading"));
    sym_writing = ID2SYM(rb_intern("writing"));
    sym_ok = ID2SYM(rb_intern("ok"));
    sym_failed = ID2SYM(rb_intern("failed"));
    sym_active = ID2SYM(rb_intern("active"));

    mPgPipeline = rb_define_module("PgPipeline");
    mNative = rb_define_module_under(mPgPipeline, "Native");

    cError = rb_const_get(mPgPipeline, rb_intern("Error"));
    cUnsupportedServerError = rb_const_get(mPgPipeline, rb_intern("UnsupportedServerError"));
    cPipelineAbortedError = rb_const_get(mPgPipeline, rb_intern("PipelineAbortedError"));
    cConnectionLostError = rb_const_get(mPgPipeline, rb_intern("ConnectionLostError"));
    cNotDispatchedError = rb_const_get(mPgPipeline, rb_intern("NotDispatchedError"));
    cIndeterminateResultError = rb_const_get(mPgPipeline, rb_intern("IndeterminateResultError"));
    cShutdownError = rb_const_get(mPgPipeline, rb_intern("ShutdownError"));
    cProtocolError = rb_const_get(mPgPipeline, rb_intern("ProtocolError"));
    cQueryError = rb_const_get(mPgPipeline, rb_intern("QueryError"));

    rb_define_singleton_method(mNative, "libpq_version", pp_native_libpq_version, 0);
    rb_define_singleton_method(mNative, "seal_encoding", pp_native_seal_encoding, 0);
    rb_define_singleton_method(mNative, "seal_encoding_published?",
                               pp_native_seal_encoding_published_p, 0);
    rb_define_singleton_method(mNative, "reset_seal_encoding!", pp_native_reset_seal_encoding, 0);

    cRequestState = rb_define_class_under(mNative, "RequestState", rb_cObject);
    rb_define_alloc_func(cRequestState, pp_request_state_alloc);
    rb_define_method(cRequestState, "initialize", pp_request_state_initialize, 0);
    rb_define_method(cRequestState, "state", pp_request_state_state, 0);
    rb_define_method(cRequestState, "state=", pp_request_state_set_state, 1);
    rb_define_method(cRequestState, "cancelled", pp_request_state_cancelled, 0);
    rb_define_method(cRequestState, "cancelled=", pp_request_state_set_cancelled, 1);
    rb_define_method(cRequestState, "settled", pp_request_state_settled, 0);
    rb_define_method(cRequestState, "settled=", pp_request_state_set_settled, 1);
    rb_define_method(cRequestState, "result_seen", pp_request_state_result_seen, 0);
    rb_define_method(cRequestState, "result_seen=", pp_request_state_set_result_seen, 1);
    rb_define_method(cRequestState, "query_boundary_seen", pp_request_state_query_boundary_seen, 0);
    rb_define_method(cRequestState,
                     "query_boundary_seen=", pp_request_state_set_query_boundary_seen, 1);
    rb_define_method(cRequestState, "result", pp_request_state_result, 0);
    rb_define_method(cRequestState, "result=", pp_request_state_set_result, 1);
    rb_define_method(cRequestState, "error", pp_request_state_error, 0);
    rb_define_method(cRequestState, "error=", pp_request_state_set_error, 1);
    rb_define_method(cRequestState, "waiter", pp_request_state_waiter, 0);
    rb_define_method(cRequestState, "waiter=", pp_request_state_set_waiter, 1);
    rb_define_method(cRequestState, "waiter_scheduler", pp_request_state_waiter_scheduler, 0);
    rb_define_method(cRequestState, "waiter_scheduler=", pp_request_state_set_waiter_scheduler, 1);
    rb_define_method(cRequestState, "transition!", pp_request_state_transition, 2);
    rb_define_method(cRequestState, "accept_result", pp_request_state_accept_result, 1);
    rb_define_method(cRequestState, "record_error!", pp_request_state_record_error, 2);
    rb_define_method(cRequestState, "query_boundary!", pp_request_state_query_boundary, 0);
    rb_define_method(cRequestState, "finish!", pp_request_state_finish, 1);
    rb_define_method(cRequestState, "reject!", pp_request_state_reject, 2);
    rb_define_method(cRequestState, "cancel!", pp_request_state_cancel, 0);
    rb_define_method(cRequestState, "wait", pp_request_state_wait, 1);
    rb_define_method(cRequestState, "seal!", pp_request_state_seal, 5);
    rb_define_method(cRequestState, "adopt_payload!", pp_request_state_adopt_payload, 1);
    rb_define_method(cRequestState, "sealed?", pp_request_state_sealed_p, 0);
    rb_define_method(cRequestState, "payload_digest", pp_request_state_payload_digest, 0);

    cResultBase = rb_const_get(mPgPipeline, rb_intern("Result"));
    cResult = rb_define_class_under(mNative, "Result", cResultBase);
    rb_define_alloc_func(cResult, pp_result_alloc);
    rb_include_module(cResult, rb_mEnumerable);
    rb_define_method(cResult, "clear", pp_result_clear_method, 0);
    rb_define_method(cResult, "cleared?", pp_result_cleared_p, 0);
    rb_define_method(cResult, "external_bytes", pp_result_external_bytes, 0);
    rb_define_method(cResult, "result_status", pp_result_status, 0);
    rb_define_method(cResult, "error_message", pp_result_error_message, 0);
    rb_define_method(cResult, "error_field", pp_result_error_field, 1);
    rb_define_method(cResult, "ntuples", pp_result_ntuples, 0);
    rb_define_method(cResult, "nfields", pp_result_nfields, 0);
    rb_define_method(cResult, "fields", pp_result_fields, 0);
    rb_define_method(cResult, "fname", pp_result_fname, 1);
    rb_define_method(cResult, "fnumber", pp_result_fnumber, 1);
    rb_define_method(cResult, "ftype", pp_result_ftype, 1);
    rb_define_method(cResult, "fmod", pp_result_fmod, 1);
    rb_define_method(cResult, "getvalue", pp_result_getvalue, 2);
    rb_define_method(cResult, "getisnull", pp_result_getisnull, 2);
    rb_define_method(cResult, "getlength", pp_result_getlength, 2);
    rb_define_method(cResult, "tuple_values", pp_result_tuple_values, 1);
    rb_define_method(cResult, "each_row", pp_result_each_row, 0);
    rb_define_method(cResult, "[]", pp_result_aref, 1);
    rb_define_method(cResult, "first", pp_result_first, -1);
    rb_define_method(cResult, "each", pp_result_each, 0);
    rb_define_method(cResult, "values", pp_result_values, 0);
    rb_define_method(cResult, "to_a", pp_result_to_a, 0);
    rb_define_method(cResult, "column_values", pp_result_column_values, 1);
    rb_define_method(cResult, "field_values", pp_result_field_values, 1);
    rb_define_method(cResult, "cmd_tuples", pp_result_cmd_tuples, 0);
    rb_define_method(cResult, "length", pp_result_length, 0);
    rb_define_alias(cResult, "size", "length");

    cDriver = rb_define_class_under(mNative, "Driver", rb_cObject);
    rb_define_alloc_func(cDriver, pp_driver_alloc);
    rb_define_method(cDriver, "initialize", pp_driver_initialize, 2);
    rb_define_method(cDriver, "connect_poll", pp_driver_connect_poll, 0);
    rb_define_method(cDriver, "socket", pp_driver_socket, 0);
    rb_define_method(cDriver, "encoding", pp_driver_encoding, 0);
    rb_define_method(cDriver, "error_message", pp_driver_error_message, 0);
    rb_define_method(cDriver, "protocol_version", pp_driver_protocol_version, 0);
    rb_define_method(cDriver, "server_version", pp_driver_server_version, 0);
    rb_define_method(cDriver, "status", pp_driver_status, 0);
    rb_define_method(cDriver, "pipeline_status", pp_driver_pipeline_status, 0);
    rb_define_method(cDriver, "transaction_status", pp_driver_transaction_status, 0);
    rb_define_method(cDriver, "enter_pipeline_mode", pp_driver_enter_pipeline, 0);
    rb_define_method(cDriver, "exit_pipeline_mode", pp_driver_exit_pipeline, 0);
    rb_define_method(cDriver, "close", pp_driver_close, 0);
    rb_define_method(cDriver, "closed?", pp_driver_closed_p, 0);
    rb_define_method(cDriver, "inflight_count", pp_driver_inflight_count, 0);
    rb_define_method(cDriver, "inflight_plus", pp_driver_inflight_plus, 1);
    rb_define_method(cDriver, "reusable?", pp_driver_reusable_p, 0);
    rb_define_method(cDriver, "pipeline_aborted?", pp_driver_pipeline_aborted_p, 0);
    rb_define_method(cDriver, "dispatch", pp_driver_dispatch, 1);
    rb_define_method(cDriver, "flush", pp_driver_flush, 0);
    rb_define_method(cDriver, "consume_and_drain", pp_driver_consume_and_drain, 0);
    rb_define_method(cDriver, "drain", pp_driver_drain_method, 0);
    rb_define_method(cDriver, "front_request", pp_driver_front_request, 0);
    rb_define_method(cDriver, "take_inflight", pp_driver_take_inflight, 0);
    rb_define_method(cDriver, "stats", pp_driver_stats, 0);
    rb_define_method(cDriver, "counter", pp_driver_counter, 1);

    {
        void pp_session_guard_init(VALUE);
        void pp_bounded_queue_init(VALUE);
        pp_session_guard_init(mPgPipeline);
        pp_bounded_queue_init(mPgPipeline);
    }
}
