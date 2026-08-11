# frozen_string_literal: true

module PgPipeline
  # Up to 0.3.1 the multiplexed path handed back a `PG::Result` directly. 0.4
  # returns a `PgPipeline::Result` instead, because the native driver owns the
  # `PGresult` itself and never builds a ruby-pg object for it. The row and
  # metadata surface below is the part of `PG::Result` that callers actually
  # used, spelled the same way -- including ruby-pg's `num_tuples` / `num_fields`
  # / `each_row` aliases -- so most call sites need no change. What cannot be
  # carried over is `is_a?(PG::Result)` and ruby-pg's type-map machinery
  # (`map_types!`, `PG::BasicTypeMapForResults`); see the 0.4.0 CHANGELOG.
  class Result
    include Enumerable

    def self.wrap(result)
      return result if result.is_a?(Result)

      RubyResult.new(result)
    end

    # ruby-pg spellings, defined once in terms of the primitives each subclass
    # implements, so the native and pinned paths cannot drift apart.
    def num_tuples = ntuples
    def num_fields = nfields

    def each_row
      return enum_for(:each_row) unless block_given?

      index = 0
      count = ntuples
      while index < count
        yield tuple_values(index)
        index += 1
      end
      self
    end
  end

  class RubyResult < Result
    def initialize(result)
      @result = result
    end

    def clear
      result = @result
      @result = nil
      result&.clear
      nil
    end

    def cleared? = @result.nil?

    # Only the native result owns a PGresult whose size the GC needs told about;
    # ruby-pg accounts for its own. Defined here so callers can read it off any
    # result without branching on the class.
    def external_bytes = 0

    def result_status = raw.result_status
    def error_message = raw.error_message
    def error_field(code) = raw.error_field(code)
    def ntuples = raw.ntuples
    def nfields = raw.nfields
    def fields = raw.fields
    def fname(index) = raw.fname(index)
    def fnumber(name) = raw.fnumber(name)
    def ftype(index) = raw.ftype(index)
    def fmod(index) = raw.fmod(index)
    def getvalue(row, column) = raw.getvalue(row, column)
    def getisnull(row, column) = raw.getisnull(row, column)
    def getlength(row, column) = raw.getlength(row, column)
    def tuple_values(row) = raw.tuple_values(row)
    def column_values(index) = raw.column_values(index)
    def field_values(name) = raw.field_values(name)
    def values = raw.values
    def cmd_tuples = raw.cmd_tuples
    def length = raw.ntuples
    alias size length

    def [](index) = raw[index]
    def first(*args) = raw.first(*args)

    def each
      return enum_for(:each) unless block_given?

      raw.each { |row| yield row }
      self
    end

    def to_a = map { |row| row }

    private

    def raw
      @result || raise(ProtocolError, "result has been cleared")
    end
  end
end
