# frozen_string_literal: true

module PgPipeline
  class Error < StandardError; end
  class UnsupportedServerError < Error; end
  class UnsafeMultiplexError < Error; end
  class PipelineAbortedError < Error; end
  class ConnectionLostError < Error; end
  class NotDispatchedError < ConnectionLostError; end
  class IndeterminateResultError < ConnectionLostError; end
  class ShutdownError < Error; end
  class ProtocolError < Error; end

  class QueryError < Error
    attr_reader :cause_result

    def initialize(message, cause_result: nil)
      super(message)
      @cause_result = cause_result
    end

    def clear_result!
      @cause_result&.clear if @cause_result.respond_to?(:clear)
      @cause_result = nil
    end
  end
end
