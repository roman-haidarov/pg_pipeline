# frozen_string_literal: true

require "spec_helper"

RSpec.describe PgPipeline::PoolOps do
  let(:driver_class) do
    Struct.new(:available, :load) do
      def available? = available
    end
  end

  def build(available, load)
    driver_class.new(available, load)
  end

  describe ".select_driver_into" do
    it "returns the least-loaded driver and writes the next cursor" do
      first = build(true, 5)
      second = build(true, 1)
      slot = [0]

      expect(described_class.select_driver_into([first, second], 0, slot)).to equal(second)
      expect(slot[0]).to eq(0)
    end

    it "reports the unchanged cursor when there is no live driver" do
      slot = [3]

      expect(described_class.select_driver_into([build(false, 0)], 7, slot)).to be_nil
      expect(slot[0]).to eq(7)
    end

    it "prefers an idle driver over loaded peers" do
      idle = build(true, 0)
      loaded = build(true, 3)
      drivers = [loaded, idle, loaded]
      slot = [0]

      expect(described_class.select_driver_into(drivers, 0, slot)).to equal(idle)
      expect(slot[0]).to eq(2)
    end

    it "normalises a cursor that is past the end of the pool" do
      only = build(true, 2)
      slot = [0]

      expect(described_class.select_driver_into([only], 7, slot)).to equal(only)
      expect(slot[0]).to eq(0)
    end

    it "handles an empty driver list" do
      slot = [1]

      expect(described_class.select_driver_into([], 4, slot)).to be_nil
      expect(slot[0]).to eq(4)
    end

    it "prefers the round-robin start when loads are equal" do
      drivers = [build(true, 2), build(true, 2), build(true, 2)]
      slot = [0]

      expect(described_class.select_driver_into(drivers, 1, slot)).to equal(drivers[1])
      expect(slot[0]).to eq(2)
    end

    def reference_select(drivers, cursor)
      size = drivers.length
      return [nil, cursor] if size.zero?

      best = nil
      best_index = nil
      size.times do |offset|
        index = (cursor + offset) % size
        driver = drivers[index]
        next unless driver.available?
        next unless best.nil? || driver.load < best.load

        best = driver
        best_index = index
      end

      best ? [best, (best_index + 1) % size] : [nil, cursor]
    end

    it "agrees with an independent reference on randomised pools" do
      random = Random.new(7)

      2000.times do
        size = random.rand(0..5)
        drivers = Array.new(size) { build(random.rand(4).positive?, random.rand(0..3)) }
        cursor = random.rand(0..12)

        expected_driver, expected_cursor = reference_select(drivers, cursor)
        slot = [0]
        actual_driver = described_class.select_driver_into(drivers, cursor, slot)
        tuple_driver, tuple_cursor = described_class.select_driver(drivers, cursor)

        expect(actual_driver).to equal(expected_driver)
        expect(slot[0]).to eq(expected_cursor)
        expect(tuple_driver).to equal(expected_driver)
        expect(tuple_cursor).to eq(expected_cursor)
      end
    end
  end
end
