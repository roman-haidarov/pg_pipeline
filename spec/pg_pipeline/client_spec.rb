# frozen_string_literal: true

require "spec_helper"

RSpec.describe PgPipeline::ClientOps do
  describe ".submit_with_failover" do
    def client_double(pipeline_size:)
      pool = instance_double(PgPipeline::Pool, pipeline_size: pipeline_size)
      client = PgPipeline::Client.allocate
      client.instance_variable_set(:@pool, pool)
      client.instance_variable_set(:@guard, :default)
      [client, pool]
    end

    it "returns the request when the first driver accepts it" do
      Async do
        client, pool = client_double(pipeline_size: 2)
        driver = instance_double(PgPipeline::ConnectionDriver)
        allow(pool).to receive(:pipeline_driver).and_return(driver)
        allow(driver).to receive(:submit) { |request| request }

        request = described_class.submit_with_failover(client, "SELECT 1", [])
        expect(request).to be_a(PgPipeline::Request)
        expect(driver).to have_received(:submit).once
      end.wait
    end

    it "retries on a fresh Request when the driver rejects pre-dispatch" do
      Async do
        client, pool = client_double(pipeline_size: 2)
        dead = instance_double(PgPipeline::ConnectionDriver)
        live = instance_double(PgPipeline::ConnectionDriver)
        calls = 0

        allow(pool).to receive(:pipeline_driver) do
          calls += 1
          calls == 1 ? dead : live
        end

        allow(dead).to receive(:submit) do |_request|
          raise PgPipeline::ShutdownError, "driver is not accepting work"
        end
        allow(live).to receive(:submit) { |request| request.queued!; request }

        request = described_class.submit_with_failover(client, "SELECT 1", [])
        expect(request.state).to eq(:queued)
        expect(calls).to eq(2)
      end.wait
    end

    it "retries a settled NotDispatchedError on a fresh Request" do
      Async do
        client, pool = client_double(pipeline_size: 2)
        dead = instance_double(PgPipeline::ConnectionDriver)
        live = instance_double(PgPipeline::ConnectionDriver)
        calls = 0

        allow(pool).to receive(:pipeline_driver) do
          calls += 1
          calls == 1 ? dead : live
        end

        allow(dead).to receive(:submit) do |request|
          request.reject!(PgPipeline::NotDispatchedError.new("queue closed; request was not dispatched"))
          raise PgPipeline::NotDispatchedError, "queue closed; request was not dispatched"
        end
        allow(live).to receive(:submit) { |request| request.queued!; request }

        request = described_class.submit_with_failover(client, "SELECT 1", [])
        expect(request.state).to eq(:queued)
        expect(calls).to eq(2)
      end.wait
    end

    it "raises NotDispatchedError when every attempt fails pre-dispatch" do
      Async do
        client, pool = client_double(pipeline_size: 2)
        driver = instance_double(PgPipeline::ConnectionDriver)
        allow(pool).to receive(:pipeline_driver).and_return(driver)
        allow(driver).to receive(:submit)
          .and_raise(PgPipeline::NotDispatchedError, "no live pipeline connections")

        expect {
          described_class.submit_with_failover(client, "SELECT 1", [])
        }.to raise_error(PgPipeline::NotDispatchedError)

        expect(driver).to have_received(:submit).twice
      end.wait
    end
  end

  describe "public surface" do
    it "rejects unknown guard modes instead of falling back to default" do
      expect { PgPipeline::Client.new(nil, guard: :strcit) }
        .to raise_error(ArgumentError, /guard must be one of/)
    end

    it "does not expose the internal pool through the public Client API" do
      client = PgPipeline::Client.new(nil)
      expect(client).not_to respond_to(:pool)
      expect(client).to respond_to(:stats)
    end

    it "does not expose lifecycle ownership state through public accessors" do
      client = PgPipeline::Client.new(nil)

      expect(client).not_to respond_to(:started)
      expect(client).not_to respond_to(:started=)
      expect(client).not_to respond_to(:owner_thread)
      expect(client).not_to respond_to(:owner_thread=)
      expect(client).not_to respond_to(:scheduler)
      expect(client).not_to respond_to(:scheduler=)
    end
  end

end
