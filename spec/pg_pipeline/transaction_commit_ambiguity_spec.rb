# frozen_string_literal: true

require "spec_helper"

RSpec.describe PgPipeline::Transaction, "COMMIT acknowledgement ambiguity" do
  def commit_failure_connection(error, status: PG::CONNECTION_OK, finished: false)
    Class.new do
      attr_reader :calls

      define_method(:initialize) do
        @calls = []
      end

      define_method(:exec) do |sql|
        @calls << [:exec, sql]
        raise error if sql == "COMMIT"

        :ok
      end

      define_method(:status) { status }
      define_method(:finished?) { finished }
    end.new
  end

  it "types a lost COMMIT acknowledgement as an indeterminate result" do
    conn = commit_failure_connection(PG::ConnectionBad.new("connection lost"))
    tx = described_class.new(conn)

    expect { tx.run { :done } }
      .to raise_error(PgPipeline::IndeterminateCommitError, /must not be retried blindly/)

    expect(PgPipeline::IndeterminateCommitError < PgPipeline::IndeterminateResultError).to be(true)
    expect(conn.calls).to eq([
      [:exec, "BEGIN"],
      [:exec, "COMMIT"],
      [:exec, "ROLLBACK"]
    ])
  end

  it "does not relabel a server-side COMMIT error while the connection is healthy" do
    conn = commit_failure_connection(PG::Error.new("commit rejected"))
    tx = described_class.new(conn)

    expect { tx.run { :done } }
      .to raise_error(PG::Error, "commit rejected")
  end
end
