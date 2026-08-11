# frozen_string_literal: true

require "spec_helper"

# 0.4 deleted the pure-Ruby multiplexed driver, so `backend:` no longer means
# anything. Ruby's own "unknown keyword: :backend" is technically accurate and
# tells a 0.3 caller nothing about where the option went or what to do next --
# and this is the one option they are likely to have set on purpose.
RSpec.describe PgPipeline::Pool do
  describe "options removed in 0.4.0" do
    it "explains where backend: went instead of raising a bare unknown keyword" do
      expect { described_class.new("postgres:///x", backend: :ruby) }
        .to raise_error(ArgumentError, /backend:.*removed in 0\.4\.0/m)
    end

    it "names the version to pin for a compiler-free multiplexed driver" do
      expect { described_class.new("postgres:///x", backend: :ruby) }
        .to raise_error(ArgumentError, /0\.3\.1/)
    end

    it "still rejects a genuinely unknown option" do
      expect { described_class.new("postgres:///x", nonsense: 1) }
        .to raise_error(ArgumentError, /unknown keyword: :nonsense/)
    end

    it "lists every unknown option rather than only the first" do
      expect { described_class.new("postgres:///x", nonsense: 1, drivel: 2) }
        .to raise_error(ArgumentError, /:nonsense.*:drivel/m)
    end

    # The check runs before anything else in the constructor, so a caller
    # migrating from 0.3 gets the explanation rather than a libpq error.
    it "reports the removal before touching libpq" do
      expect(PgPipeline::Native).not_to receive(:assert_libpq_compatible!)

      expect { described_class.new("postgres:///x", backend: :ruby) }
        .to raise_error(ArgumentError)
    end
  end

  describe "supported options" do
    it "accepts the documented keywords" do
      expect { described_class.new("postgres:///x", pipeline_size: 2, pinned_size: 1) }
        .not_to raise_error
    end
  end
end
