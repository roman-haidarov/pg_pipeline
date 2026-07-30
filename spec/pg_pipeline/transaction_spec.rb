# frozen_string_literal: true

require "spec_helper"

RSpec.describe PgPipeline::Transaction do
  class TxSpecConnection
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

    def quote_ident(name)
      %("#{name.gsub('"', '""')}")
    end
  end

  it "commits on success and rolls back on failure" do
    conn = TxSpecConnection.new
    tx = described_class.new(conn)

    expect(tx.run { |t| t.exec("SELECT 1"); :done }).to eq(:done)
    expect(conn.calls).to eq([
      [:exec, "BEGIN"],
      [:exec, "SELECT 1"],
      [:exec, "COMMIT"]
    ])

    conn.calls.clear
    expect {
      tx.run { raise "boom" }
    }.to raise_error(RuntimeError, "boom")
    expect(conn.calls).to eq([
      [:exec, "BEGIN"],
      [:exec, "ROLLBACK"]
    ])
  end

  it "supports nested savepoints with rollback to savepoint" do
    conn = TxSpecConnection.new
    tx = described_class.new(conn)

    tx.run do |outer|
      outer.exec("INSERT 1")
      begin
        outer.savepoint do |sp|
          sp.exec("INSERT 2")
          raise "inner"
        end
      rescue RuntimeError
        nil
      end
      outer.exec("SELECT count")
    end

    expect(conn.calls).to eq([
      [:exec, "BEGIN"],
      [:exec, "INSERT 1"],
      [:exec, 'SAVEPOINT "pgp_sp_1"'],
      [:exec, "INSERT 2"],
      [:exec, 'ROLLBACK TO SAVEPOINT "pgp_sp_1"'],
      [:exec, 'RELEASE SAVEPOINT "pgp_sp_1"'],
      [:exec, "SELECT count"],
      [:exec, "COMMIT"]
    ])
  end

  it "cannot start or manipulate a transaction from another fiber" do
    conn = TxSpecConnection.new
    tx = described_class.new(conn)
    run_error = nil

    Fiber.new do
      begin
        tx.run { }
      rescue => e
        run_error = e
      end
    end.resume

    expect(run_error).to be_a(PgPipeline::Error)
    expect(run_error.message).to match(/fiber-local/)
    expect(conn.calls).to be_empty

    tx.__send__(:open=, true)
    savepoint_error = nil
    Fiber.new do
      begin
        tx.savepoint { }
      rescue => e
        savepoint_error = e
      end
    end.resume

    expect(savepoint_error).to be_a(PgPipeline::Error)
    expect(savepoint_error.message).to match(/fiber-local/)
    expect(conn.calls).to be_empty
  end

  it "rejects a nested run that would commit the outer transaction early" do
    conn = TxSpecConnection.new
    tx = described_class.new(conn)

    tx.run do |outer|
      expect { outer.run { } }
        .to raise_error(PgPipeline::Error, /already open/)
      outer.exec("SELECT 1")
    end

    expect(conn.calls).to eq([
      [:exec, "BEGIN"],
      [:exec, "SELECT 1"],
      [:exec, "COMMIT"]
    ])
  end

  it "rejects savepoint outside an open transaction" do
    tx = described_class.new(TxSpecConnection.new)
    expect { tx.savepoint { } }.to raise_error(PgPipeline::Error, /open transaction/)
  end

  it "does not expose transaction control state for external mutation (P1)" do
    tx = described_class.new(TxSpecConnection.new)

    expect(tx.open?).to be(false)
    expect(tx).not_to respond_to(:open=)
    expect(tx).not_to respond_to(:savepoint_seq)
    expect(tx).not_to respond_to(:savepoint_seq=)
    expect { tx.open = true }.to raise_error(NoMethodError)
  end
end
