# frozen_string_literal: true

require "spec_helper"

RSpec.describe PgPipeline::Session do
  class SessionSpecConnection
    attr_reader :calls

    def initialize
      @calls = []
    end

    def exec(sql)
      @calls << [:exec, sql]
      :ok
    end

    def exec_params(sql, params)
      @calls << [:exec_params, sql, params]
      :ok
    end
  end

  it "does not expose the reusable raw PG connection" do
    session = described_class.new(SessionSpecConnection.new)

    expect(session).not_to respond_to(:conn)
    expect(session).not_to respond_to(:conn=)
    expect(session).not_to respond_to(:close!)
  end

  it "cannot be used from another fiber" do
    conn = SessionSpecConnection.new
    session = described_class.new(conn)
    error = nil

    Fiber.new do
      begin
        session.exec("SELECT 1")
      rescue => e
        error = e
      end
    end.resume

    expect(error).to be_a(PgPipeline::Error)
    expect(error.message).to match(/fiber-local/)
    expect(conn.calls).to be_empty
  end

  it "cannot be used after its scope is invalidated" do
    conn = SessionSpecConnection.new
    session = described_class.new(conn)

    expect(session.exec("SELECT 1")).to eq(:ok)
    PgPipeline::SessionOps.close!(session)

    expect { session.exec("SELECT 2") }
      .to raise_error(PgPipeline::Error, /no longer active/)
    expect(conn.calls).to eq([[:exec, "SELECT 1"]])
  end
end
