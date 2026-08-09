# frozen_string_literal: true

require_relative "errors"

module PgPipeline
  module SessionGuard
    module_function

    VALID_MODES = %i[default strict].freeze
    ALLOWED_LEADING = %w[select insert update delete merge values with].freeze
    FORBIDDEN_PATTERNS = {
      "set_config" => /\bset_config\s*\(/i,
      "setseed" => /\bsetseed\s*\(/i,
      "currval" => /\bcurrval\s*\(/i,
      "lastval" => /\blastval\s*\(/i,
      "session-advisory-lock" => /\bpg_(?:try_)?advisory_lock(?:_shared)?\s*\(/i,
      "session-advisory-unlock" => /\bpg_advisory_unlock(?:_shared|_all)?\s*\(/i,
      "select-into-temp" => /\binto\s+(?:(?:global|local)\s+)?temp(?:orary)?\b/i,
      "select-into-pg-temp" => /\binto\s+(?:table\s+)?pg_temp(?:_\d+)?\s*\./i
    }.freeze
    STRICT_FORBIDDEN = {
      "nextval" => /\bnextval\s*\(/i,
      "setval" => /\bsetval\s*\(/i,
      "setseed" => /\bsetseed\s*\(/i,
      "set_config" => /\bset_config\s*\(/i,
      "session-advisory-lock" => /\bpg_(?:try_)?advisory_lock(?:_shared)?\s*\(/i,
      "session-advisory-unlock" => /\bpg_advisory_unlock(?:_shared|_all)?\s*\(/i,
      "pg_export_snapshot" => /\bpg_export_snapshot\s*\(/i
    }.freeze
    PATTERN_PREFILTER = /\binto\b/i
    DOLLAR_QUOTE_TAG = /\A\$(?:(?:[A-Za-z_]|[^\x00-\x7F])(?:[A-Za-z0-9_]|[^\x00-\x7F])*)?\$/
    NEEDS_MASK = /['"]|--|\/\*|\$(?:(?:[A-Za-z_]|[^\x00-\x7F])(?:[A-Za-z0-9_]|[^\x00-\x7F])*)?\$/
    LEADING_KEYWORD = /\A\s*([a-zA-Z_]+)/
    WHITESPACE_BYTES = [9, 10, 11, 12, 13, 32].freeze

    def assert_multiplexable!(sql, mode: :default)
      assert_multiplexable_normalized!(sql, mode: normalize_mode!(mode))
    end

    def assert_multiplexable_normalized!(sql, mode:)
      reason = unsafe_reason_normalized(sql, mode: mode)
      return true unless reason

      raise UnsafeMultiplexError,
            "refusing to multiplex SQL (#{reason}); the shared path only accepts " \
            "session-neutral operations. Use Client#session for session work or " \
            "Client#transaction for an explicit transaction.\n" \
            "  offending SQL: #{sql.to_s.strip[0, 160]}"
    end

    GUARD_CACHE_LIMIT = 2048
    SAFE = :safe

    def unsafe_reason(sql, mode: :default)
      unsafe_reason_normalized(sql, mode: normalize_mode!(mode))
    end

    def unsafe_reason_normalized(sql, mode:)
      key = sql.to_s
      cache = guard_cache.fetch(mode)
      cached = cache[key]
      return (cached.equal?(SAFE) ? nil : cached) if cached

      reason = compute_unsafe_reason(key, mode)
      cache.shift if cache.size >= GUARD_CACHE_LIMIT
      cache[key] = reason || SAFE
      reason
    end

    def guard_cache
      @guard_cache ||= { default: {}, strict: {} }
    end

    def compute_unsafe_reason(sql, mode)
      code = NEEDS_MASK.match?(sql) ? code_only(sql) : sql
      lead = code[LEADING_KEYWORD, 1]&.downcase

      return "empty" unless lead
      return "leading:#{lead}" unless ALLOWED_LEADING.include?(lead)
      return "multiple-statements" if multiple_statements?(code)
      return nil unless code.include?("(") || code.match?(PATTERN_PREFILTER)

      FORBIDDEN_PATTERNS.each do |name, pattern|
        return name if code.match?(pattern)
      end
      if mode == :strict
        STRICT_FORBIDDEN.each do |name, pattern|
          return "strict:#{name}" if code.match?(pattern)
        end
      end

      nil
    end

    def normalize_mode!(mode)
      normalized = mode.respond_to?(:to_sym) ? mode.to_sym : mode
      return normalized if VALID_MODES.include?(normalized)

      raise ArgumentError, "guard must be one of: #{VALID_MODES.map(&:inspect).join(", ")}"
    end

    def code_only(sql)
      source = sql.to_s.b
      size = source.bytesize
      output = String.new(capacity: size, encoding: Encoding::BINARY)
      index = 0
      block_depth = 0

      while index < size
        byte = source.getbyte(index)
        nxt = index + 1 < size ? source.getbyte(index + 1) : nil
        if block_depth.positive?
          if byte == 47 && nxt == 42
            block_depth += 1
            output << "  "
            index += 2
          elsif byte == 42 && nxt == 47
            block_depth -= 1
            output << "  "
            index += 2
          else
            output << (byte == 10 ? 10 : 32)
            index += 1
          end
          next
        end

        if byte == 45 && nxt == 45
          newline = source.index("\n", index + 2)
          if newline
            output << (" " * (newline - index)) << "\n"
            index = newline + 1
          else
            output << (" " * (size - index))
            break
          end
          next
        end

        if byte == 47 && nxt == 42
          block_depth = 1
          output << "  "
          index += 2
          next
        end

        if byte == 39
          index = mask_quoted(source, output, index, 39, escape_backslash: escape_string_prefix?(source, index))
          next
        end

        if byte == 34
          index = mask_quoted(source, output, index, 34, escape_backslash: false)
          next
        end

        if byte == 36
          remainder = source.byteslice(index, size - index)
          tag = DOLLAR_QUOTE_TAG.match(remainder)&.[](0)
          if tag
            closing = source.index(tag, index + tag.bytesize)
            finish = closing ? closing + tag.bytesize : size
            output << (" " * (finish - index))
            index = finish
            next
          end
        end

        output << byte
        index += 1
      end

      output
    end

    def mask_quoted(source, output, index, quote_byte, escape_backslash:)
      output << " "
      index += 1

      while index < source.bytesize
        byte = source.getbyte(index)
        if escape_backslash && byte == 92
          output << " "
          index += 1
          if index < source.bytesize
            output << (source.getbyte(index) == 10 ? "\n" : " ")
            index += 1
          end
        elsif byte == quote_byte
          if source.getbyte(index + 1) == quote_byte
            output << "  "
            index += 2
          else
            output << " "
            return index + 1
          end
        else
          output << (byte == 10 ? "\n" : " ")
          index += 1
        end
      end
      index
    end
    private_class_method :mask_quoted

    def escape_string_prefix?(source, quote_index)
      return false if quote_index.zero?

      marker = source.getbyte(quote_index - 1)
      return false unless marker == 69 || marker == 101

      before = quote_index >= 2 ? source.getbyte(quote_index - 2) : nil
      before.nil? || !(before.between?(48, 57) || before.between?(65, 90) || before.between?(97, 122) || before == 95)
    end
    private_class_method :escape_string_prefix?

    def multiple_statements?(code)
      first = code.index(";")
      return false unless first

      index = first + 1
      size = code.bytesize
      while index < size
        return true unless WHITESPACE_BYTES.include?(code.getbyte(index))

        index += 1
      end
      false
    end
  end
end
