#include "ruby.h"
#include "ruby/encoding.h"
#include <ctype.h>
#include <string.h>

#define GUARD_CACHE_LIMIT 2048

static VALUE mSessionGuard;
static VALUE cUnsafeMultiplexError;
static VALUE cArgError;
static VALUE sym_default;
static VALUE sym_strict;
static VALUE sym_safe;
static VALUE default_cache;
static VALUE default_cache_old;
static VALUE strict_cache;
static VALUE strict_cache_old;

static int is_ws(unsigned char c) {
    return c == 9 || c == 10 || c == 11 || c == 12 || c == 13 || c == 32;
}

static int is_ident(unsigned char c) {
    return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_' ||
           c >= 0x80;
}

static int ascii_ieq(const char *a, const char *b, size_t n) {
    for (size_t i = 0; i < n; i++) {
        unsigned char ca = (unsigned char)a[i];
        unsigned char cb = (unsigned char)b[i];
        if (ca >= 'A' && ca <= 'Z')
            ca = (unsigned char)(ca + 32);
        if (cb >= 'A' && cb <= 'Z')
            cb = (unsigned char)(cb + 32);
        if (ca != cb)
            return 0;
    }
    return 1;
}

static int word_bound_before(const char *s, long i) {
    return i <= 0 || !is_ident((unsigned char)s[i - 1]);
}

static int word_bound_after(const char *s, long len, long i) {
    return i >= len || !is_ident((unsigned char)s[i]);
}

static long skip_ws(const char *s, long len, long i) {
    while (i < len && is_ws((unsigned char)s[i]))
        i++;
    return i;
}

static int match_call(const char *s, long len, const char *kw) {
    size_t klen = strlen(kw);
    for (long i = 0; i + (long)klen < len; i++) {
        if (!word_bound_before(s, i))
            continue;
        if (!ascii_ieq(s + i, kw, klen))
            continue;
        if (!word_bound_after(s, len, i + (long)klen))
            continue;
        long j = skip_ws(s, len, i + (long)klen);
        if (j < len && s[j] == '(')
            return 1;
    }
    return 0;
}

static int match_into_temp(const char *s, long len) {
    for (long i = 0; i + 4 < len; i++) {
        if (!word_bound_before(s, i))
            continue;
        if (!ascii_ieq(s + i, "into", 4))
            continue;
        if (!word_bound_after(s, len, i + 4))
            continue;
        long j = skip_ws(s, len, i + 4);
        if (j + 6 <= len && ascii_ieq(s + j, "global", 6) && word_bound_after(s, len, j + 6))
            j = skip_ws(s, len, j + 6);
        else if (j + 5 <= len && ascii_ieq(s + j, "local", 5) && word_bound_after(s, len, j + 5))
            j = skip_ws(s, len, j + 5);
        if (j + 9 <= len && ascii_ieq(s + j, "temporary", 9) && word_bound_after(s, len, j + 9))
            return 1;
        if (j + 4 <= len && ascii_ieq(s + j, "temp", 4) && word_bound_after(s, len, j + 4))
            return 1;
    }
    return 0;
}

static int match_into_pg_temp(const char *s, long len) {
    for (long i = 0; i + 4 < len; i++) {
        if (!word_bound_before(s, i))
            continue;
        if (!ascii_ieq(s + i, "into", 4))
            continue;
        if (!word_bound_after(s, len, i + 4))
            continue;
        long j = skip_ws(s, len, i + 4);
        if (j + 5 <= len && ascii_ieq(s + j, "table", 5) && word_bound_after(s, len, j + 5))
            j = skip_ws(s, len, j + 5);
        if (j + 7 > len || !ascii_ieq(s + j, "pg_temp", 7))
            continue;
        long k = j + 7;
        if (k < len && s[k] == '_') {
            k++;
            while (k < len && s[k] >= '0' && s[k] <= '9')
                k++;
        }
        long d = skip_ws(s, len, k);
        if (d < len && s[d] == '.')
            return 1;
    }
    return 0;
}

static long dollar_tag_len(const char *s, long len, long index);

static int needs_mask(const char *s, long len) {
    for (long i = 0; i < len; i++) {
        unsigned char c = (unsigned char)s[i];
        if (c == '\'' || c == '"')
            return 1;
        if (c == '-' && i + 1 < len && s[i + 1] == '-')
            return 1;
        if (c == '/' && i + 1 < len && s[i + 1] == '*')
            return 1;
        if (c == '$' && dollar_tag_len(s, len, i) > 0)
            return 1;
    }
    return 0;
}

static int escape_string_prefix(const char *s, long quote_index) {
    if (quote_index <= 0)
        return 0;
    unsigned char marker = (unsigned char)s[quote_index - 1];
    if (marker != 'E' && marker != 'e')
        return 0;
    if (quote_index < 2)
        return 1;
    unsigned char before = (unsigned char)s[quote_index - 2];
    return !(is_ident(before));
}

static long dollar_tag_len(const char *s, long len, long index) {
    if (index >= len || s[index] != '$')
        return 0;
    long i = index + 1;
    if (i < len && s[i] == '$')
        return 2;
    if (i >= len)
        return 0;
    unsigned char c = (unsigned char)s[i];
    if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_' || c >= 0x80))
        return 0;
    i++;
    while (i < len) {
        c = (unsigned char)s[i];
        if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
            c == '_' || c >= 0x80)
            i++;
        else
            break;
    }
    if (i < len && s[i] == '$')
        return i + 1 - index;
    return 0;
}

static long find_bytes(const char *s, long len, long from, const char *needle, long nlen) {
    if (nlen <= 0 || from > len - nlen)
        return -1;
    for (long i = from; i <= len - nlen; i++) {
        if (memcmp(s + i, needle, (size_t)nlen) == 0)
            return i;
    }
    return -1;
}

static long mask_quoted(const char *src, long len, char *out, long index, unsigned char quote,
                        int escape_backslash, int keep_identifier) {
    out[index] = ' ';
    index++;
    while (index < len) {
        unsigned char byte = (unsigned char)src[index];
        if (escape_backslash && byte == '\\') {
            out[index] = ' ';
            index++;
            if (index < len) {
                out[index] = (src[index] == '\n') ? '\n' : ' ';
                index++;
            }
        } else if (byte == quote) {
            if (index + 1 < len && (unsigned char)src[index + 1] == quote) {
                out[index] = ' ';
                out[index + 1] = ' ';
                index += 2;
            } else {
                out[index] = ' ';
                return index + 1;
            }
        } else {
            if (byte == '\n')
                out[index] = '\n';
            else if (keep_identifier)
                out[index] = (char)byte;
            else
                out[index] = ' ';
            index++;
        }
    }
    return index;
}

static int quoted_ident_is_simple(const char *src, long len, long index) {
    long i = index + 1;
    long body = 0;
    while (i < len) {
        unsigned char byte = (unsigned char)src[i];
        if (byte == '"') {
            if (i + 1 < len && (unsigned char)src[i + 1] == '"')
                return 0;
            return body > 0;
        }
        if (!is_ident(byte))
            return 0;
        body++;
        i++;
    }
    return 0;
}

static int unicode_ident_prefix(const char *s, long quote_index) {
    if (quote_index < 2)
        return 0;
    unsigned char amp = (unsigned char)s[quote_index - 1];
    unsigned char u = (unsigned char)s[quote_index - 2];
    if (amp != '&' || (u != 'u' && u != 'U'))
        return 0;
    return word_bound_before(s, quote_index - 2);
}

static VALUE code_only_ex(VALUE sql, int *escaped_identifier, int *unicode_literal) {
    StringValue(sql);
    const char *src = RSTRING_PTR(sql);
    long len = RSTRING_LEN(sql);
    VALUE out = rb_str_new(NULL, len);
    char *dst = RSTRING_PTR(out);

    if (escaped_identifier)
        *escaped_identifier = 0;
    if (unicode_literal)
        *unicode_literal = 0;

    memset(dst, ' ', (size_t)len);
    long index = 0;
    int block_depth = 0;

    while (index < len) {
        unsigned char byte = (unsigned char)src[index];
        unsigned char nxt = (index + 1 < len) ? (unsigned char)src[index + 1] : 0;

        if (block_depth > 0) {
            if (byte == '/' && nxt == '*') {
                block_depth++;
                dst[index] = ' ';
                dst[index + 1] = ' ';
                index += 2;
            } else if (byte == '*' && nxt == '/') {
                block_depth--;
                dst[index] = ' ';
                dst[index + 1] = ' ';
                index += 2;
            } else {
                dst[index] = (byte == '\n') ? '\n' : ' ';
                index++;
            }
            continue;
        }

        if (byte == '-' && nxt == '-') {
            long newline = find_bytes(src, len, index + 2, "\n", 1);
            if (newline >= 0) {
                for (long k = index; k < newline; k++)
                    dst[k] = ' ';
                dst[newline] = '\n';
                index = newline + 1;
            } else {
                for (long k = index; k < len; k++)
                    dst[k] = ' ';
                index = len;
            }
            continue;
        }

        if (byte == '/' && nxt == '*') {
            block_depth = 1;
            dst[index] = ' ';
            dst[index + 1] = ' ';
            index += 2;
            continue;
        }

        if (byte == '\'') {
            if (unicode_literal && unicode_ident_prefix(src, index))
                *unicode_literal = 1;
            index = mask_quoted(src, len, dst, index, '\'', escape_string_prefix(src, index), 0);
            continue;
        }

        if (byte == '"') {
            int simple = quoted_ident_is_simple(src, len, index);
            int unicode = unicode_ident_prefix(src, index);
            if (unicode && unicode_literal)
                *unicode_literal = 1;
            if (unicode && !simple && escaped_identifier)
                *escaped_identifier = 1;
            index = mask_quoted(src, len, dst, index, '"', 0, simple);
            continue;
        }

        if (byte == '$') {
            long tlen = dollar_tag_len(src, len, index);
            if (tlen > 0) {
                long closing = find_bytes(src, len, index + tlen, src + index, tlen);
                long finish = closing >= 0 ? closing + tlen : len;
                for (long k = index; k < finish; k++)
                    dst[k] = ' ';
                index = finish;
                continue;
            }
        }

        dst[index] = (char)byte;
        index++;
    }

    rb_str_set_len(out, len);
    rb_enc_associate_index(out, rb_ascii8bit_encindex());
    return out;
}

static VALUE code_only(VALUE sql) {
    return code_only_ex(sql, NULL, NULL);
}

static int match_word(const char *s, long len, const char *kw) {
    size_t klen = strlen(kw);
    for (long i = 0; i + (long)klen <= len; i++) {
        if (!word_bound_before(s, i))
            continue;
        if (!ascii_ieq(s + i, kw, klen))
            continue;
        if (!word_bound_after(s, len, i + (long)klen))
            continue;
        return 1;
    }
    return 0;
}

static int multiple_statements(const char *code, long len) {
    long first = find_bytes(code, len, 0, ";", 1);
    if (first < 0)
        return 0;
    long index = first + 1;
    while (index < len) {
        if (!is_ws((unsigned char)code[index]))
            return 1;
        index++;
    }
    return 0;
}

static int allowed_leading(const char *kw, long klen) {
    static const char *allowed[] = {"select", "insert", "update", "delete",
                                    "merge",  "values", "with"};
    for (size_t i = 0; i < sizeof(allowed) / sizeof(allowed[0]); i++) {
        size_t n = strlen(allowed[i]);
        if ((long)n == klen && ascii_ieq(kw, allowed[i], n))
            return 1;
    }
    return 0;
}

static VALUE compute_reason(VALUE sql, int strict) {
    StringValue(sql);
    int escaped_identifier = 0;
    int unicode_literal = 0;
    VALUE code_val = needs_mask(RSTRING_PTR(sql), RSTRING_LEN(sql))
                         ? code_only_ex(sql, &escaped_identifier, &unicode_literal)
                         : sql;
    const char *code = RSTRING_PTR(code_val);
    long len = RSTRING_LEN(code_val);

    long i = skip_ws(code, len, 0);
    if (i >= len)
        return rb_str_new_cstr("empty");

    long start = i;
    while (i < len) {
        unsigned char c = (unsigned char)code[i];
        if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_')
            i++;
        else
            break;
    }
    if (i == start)
        return rb_str_new_cstr("empty");

    if (!allowed_leading(code + start, i - start)) {
        VALUE lead = rb_str_new(code + start, i - start);
        rb_funcall(lead, rb_intern("downcase!"), 0);
        return rb_sprintf("leading:%" PRIsVALUE, lead);
    }

    if (multiple_statements(code, len))
        return rb_str_new_cstr("multiple-statements");

    if (escaped_identifier)
        return rb_str_new_cstr("unicode-escaped-identifier");

    if (unicode_literal && match_word(code, len, "uescape"))
        return rb_str_new_cstr("uescape");

    int has_paren = memchr(code, '(', (size_t)len) != NULL;
    int has_into = 0;
    for (long k = 0; k + 4 <= len; k++) {
        if (ascii_ieq(code + k, "into", 4) && word_bound_before(code, k) &&
            word_bound_after(code, len, k + 4)) {
            has_into = 1;
            break;
        }
    }
    if (!has_paren && !has_into)
        return Qnil;

    if (match_call(code, len, "set_config"))
        return rb_str_new_cstr("set_config");
    if (match_call(code, len, "setseed"))
        return rb_str_new_cstr("setseed");
    if (match_call(code, len, "currval"))
        return rb_str_new_cstr("currval");
    if (match_call(code, len, "lastval"))
        return rb_str_new_cstr("lastval");
    if (match_call(code, len, "pg_advisory_lock") ||
        match_call(code, len, "pg_try_advisory_lock") ||
        match_call(code, len, "pg_advisory_lock_shared") ||
        match_call(code, len, "pg_try_advisory_lock_shared"))
        return rb_str_new_cstr("session-advisory-lock");
    if (match_call(code, len, "pg_advisory_unlock") ||
        match_call(code, len, "pg_advisory_unlock_shared") ||
        match_call(code, len, "pg_advisory_unlock_all"))
        return rb_str_new_cstr("session-advisory-unlock");
    if (match_call(code, len, "dblink_connect") || match_call(code, len, "dblink_connect_u") ||
        match_call(code, len, "dblink_disconnect"))
        return rb_str_new_cstr("dblink-session");
    if (match_call(code, len, "lo_open") || match_call(code, len, "lo_close") ||
        match_call(code, len, "lo_creat") || match_call(code, len, "lo_create") ||
        match_call(code, len, "lo_import") || match_call(code, len, "lo_export") ||
        match_call(code, len, "lo_unlink") || match_call(code, len, "lo_read") ||
        match_call(code, len, "lo_write") || match_call(code, len, "lo_lseek") ||
        match_call(code, len, "lo_lseek64") || match_call(code, len, "lo_tell") ||
        match_call(code, len, "lo_tell64") || match_call(code, len, "lo_truncate") ||
        match_call(code, len, "lo_truncate64") || match_call(code, len, "loread") ||
        match_call(code, len, "lowrite"))
        return rb_str_new_cstr("large-object");

    if (match_into_temp(code, len))
        return rb_str_new_cstr("select-into-temp");
    if (match_into_pg_temp(code, len))
        return rb_str_new_cstr("select-into-pg-temp");

    if (strict) {
        if (match_call(code, len, "nextval"))
            return rb_str_new_cstr("strict:nextval");
        if (match_call(code, len, "setval"))
            return rb_str_new_cstr("strict:setval");
        if (match_call(code, len, "pg_export_snapshot"))
            return rb_str_new_cstr("strict:pg_export_snapshot");
    }

    RB_GC_GUARD(code_val);
    return Qnil;
}

static VALUE *cache_slots_for(VALUE mode, VALUE **old_out) {
    if (mode == sym_strict) {
        *old_out = &strict_cache_old;
        return &strict_cache;
    }
    *old_out = &default_cache_old;
    return &default_cache;
}

static VALUE sg_unsafe_reason_normalized(VALUE self, VALUE sql, VALUE mode) {
    VALUE key = rb_obj_as_string(sql);
    VALUE *old_slot;
    VALUE *young_slot = cache_slots_for(mode, &old_slot);

    VALUE cached = rb_hash_lookup2(*young_slot, key, Qundef);
    if (cached == Qundef) {
        cached = rb_hash_lookup2(*old_slot, key, Qundef);
        if (cached != Qundef)
            rb_hash_aset(*young_slot, key, cached);
    }
    if (cached != Qundef)
        return cached == sym_safe ? Qnil : cached;

    int strict = (mode == sym_strict);
    VALUE reason = compute_reason(key, strict);

    if (RHASH_SIZE(*young_slot) >= GUARD_CACHE_LIMIT) {
        *old_slot = *young_slot;
        *young_slot = rb_hash_new();
    }
    rb_hash_aset(*young_slot, key, NIL_P(reason) ? sym_safe : reason);
    return reason;
}

static VALUE sg_unsafe_reason(VALUE self, VALUE sql, VALUE mode) {
    if (NIL_P(mode))
        mode = sym_default;
    if (SYMBOL_P(mode)) {
        /* ok */
    } else if (!NIL_P(mode)) {
        mode = rb_to_symbol(rb_String(mode));
    }
    if (mode != sym_default && mode != sym_strict)
        rb_raise(cArgError, "guard must be one of: :default, :strict");
    return sg_unsafe_reason_normalized(self, sql, mode);
}

static VALUE sg_assert_multiplexable_normalized(VALUE self, VALUE sql, VALUE mode) {
    VALUE reason = sg_unsafe_reason_normalized(self, sql, mode);
    if (NIL_P(reason))
        return Qtrue;

    VALUE sql_s = rb_obj_as_string(sql);
    VALUE stripped = rb_funcall(sql_s, rb_intern("strip"), 0);
    VALUE snippet = rb_str_substr(stripped, 0, 160);
    VALUE message =
        rb_sprintf("refusing to multiplex SQL (%" PRIsVALUE "); the shared path only accepts "
                   "session-neutral operations. Use Client#session for session work or "
                   "Client#transaction for an explicit transaction.\n"
                   "  offending SQL: %" PRIsVALUE,
                   reason, snippet);
    rb_exc_raise(rb_exc_new_str(cUnsafeMultiplexError, message));
    return Qtrue;
}

static VALUE sg_assert_multiplexable(int argc, VALUE *argv, VALUE self) {
    VALUE sql, mode;
    rb_scan_args(argc, argv, "11", &sql, &mode);
    if (NIL_P(mode))
        mode = sym_default;
    else if (!SYMBOL_P(mode))
        mode = rb_to_symbol(rb_String(mode));
    if (mode != sym_default && mode != sym_strict)
        rb_raise(cArgError, "guard must be one of: :default, :strict");
    return sg_assert_multiplexable_normalized(self, sql, mode);
}

static VALUE sg_normalize_mode(VALUE self, VALUE mode) {
    if (NIL_P(mode))
        mode = sym_default;
    else if (!SYMBOL_P(mode))
        mode = rb_to_symbol(rb_String(mode));
    if (mode != sym_default && mode != sym_strict)
        rb_raise(cArgError, "guard must be one of: :default, :strict");
    return mode;
}

static VALUE sg_code_only(VALUE self, VALUE sql) {
    return code_only(rb_obj_as_string(sql));
}

static VALUE sg_clear_cache(VALUE self) {
    default_cache = rb_hash_new();
    default_cache_old = rb_hash_new();
    strict_cache = rb_hash_new();
    strict_cache_old = rb_hash_new();
    return Qnil;
}

static VALUE sg_cache_size(VALUE self) {
    return SIZET2NUM((size_t)(RHASH_SIZE(default_cache) + RHASH_SIZE(default_cache_old) +
                              RHASH_SIZE(strict_cache) + RHASH_SIZE(strict_cache_old)));
}

static VALUE sg_needs_mask_p(VALUE self, VALUE sql) {
    StringValue(sql);
    return needs_mask(RSTRING_PTR(sql), RSTRING_LEN(sql)) ? Qtrue : Qfalse;
}

void pp_session_guard_init(VALUE mPgPipeline) {
    cUnsafeMultiplexError = rb_const_get(mPgPipeline, rb_intern("UnsafeMultiplexError"));
    cArgError = rb_eArgError;
    sym_default = ID2SYM(rb_intern("default"));
    sym_strict = ID2SYM(rb_intern("strict"));
    sym_safe = ID2SYM(rb_intern("safe"));
    default_cache = rb_hash_new();
    default_cache_old = rb_hash_new();
    strict_cache = rb_hash_new();
    strict_cache_old = rb_hash_new();
    rb_gc_register_address(&default_cache);
    rb_gc_register_address(&default_cache_old);
    rb_gc_register_address(&strict_cache);
    rb_gc_register_address(&strict_cache_old);

    mSessionGuard = rb_define_module_under(mPgPipeline, "SessionGuard");
    rb_define_singleton_method(mSessionGuard, "unsafe_reason_normalized_c",
                               sg_unsafe_reason_normalized, 2);
    rb_define_singleton_method(mSessionGuard, "assert_multiplexable_normalized_c!",
                               sg_assert_multiplexable_normalized, 2);
    rb_define_singleton_method(mSessionGuard, "normalize_mode!", sg_normalize_mode, 1);
    rb_define_singleton_method(mSessionGuard, "code_only", sg_code_only, 1);
    rb_define_singleton_method(mSessionGuard, "needs_mask?", sg_needs_mask_p, 1);
    rb_define_singleton_method(mSessionGuard, "clear_cache!", sg_clear_cache, 0);
    rb_define_singleton_method(mSessionGuard, "cache_size", sg_cache_size, 0);
    rb_define_singleton_method(mSessionGuard, "unsafe_reason_c", sg_unsafe_reason, 2);
    rb_define_singleton_method(mSessionGuard, "assert_multiplexable_c!", sg_assert_multiplexable,
                               -1);
}
