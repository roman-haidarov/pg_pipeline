# frozen_string_literal: true

require_relative "errors"
require_relative "native"

module PgPipeline
  module SessionGuard
    GUARD_CACHE_LIMIT = 2048
    VALID_MODES = %i[default strict].freeze

    class << self
      def assert_multiplexable!(sql, mode: :default)
        assert_multiplexable_normalized!(sql, mode: normalize_mode!(mode))
      end

      def assert_multiplexable_normalized!(sql, mode:)
        assert_multiplexable_normalized_c!(sql, mode)
      end

      def unsafe_reason(sql, mode: :default)
        unsafe_reason_normalized(sql, mode: normalize_mode!(mode))
      end

      def unsafe_reason_normalized(sql, mode:)
        unsafe_reason_normalized_c(sql, mode)
      end
    end
  end
end
