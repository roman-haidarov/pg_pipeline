# frozen_string_literal: true

require_relative "lib/pg_pipeline/version"

Gem::Specification.new do |spec|
  spec.name = "pg_pipeline"
  spec.version = PgPipeline::VERSION
  spec.authors = ["Roman Hajdarov"]
  spec.email   = ["romanhajdarov@gmail.com"]
  spec.summary = "Async-native PostgreSQL pipeline multiplexing on top of ruby-pg"
  spec.description = <<~DESC
    A driver-adjacent Ruby control-plane over ruby-pg/libpq. It multiplexes
    independent, session-neutral extended-protocol operations from many Async
    fibers onto a small number of PostgreSQL connections while keeping explicit
    transactions and session-changing work on exclusive pinned connections.
    Control-plane only: all wire work stays in libpq. Zero lines of C.
  DESC
  spec.homepage = "https://github.com/roman-haidarov/pg_pipeline"
  spec.license  = "MIT"
  spec.metadata = {
    "homepage_uri"     => "https://github.com/roman-haidarov/pg_pipeline",
    "source_code_uri"  => "https://github.com/roman-haidarov/pg_pipeline/tree/main",
    "changelog_uri"    => "https://github.com/roman-haidarov/pg_pipeline/blob/main/CHANGELOG.md",
    "bug_tracker_uri"  => "https://github.com/roman-haidarov/pg_pipeline/issues"
  }

  # --- Version floors (see DESIGN.md "Version policy") -----------------------
  #
  # Deliberately Async-native. We depend on the CURRENT async line and its
  # coordination primitives (Task/Queue/LimitedQueue+#close/Notification/
  # Semaphore/cancellation) rather than reinventing them to cling to an older
  # floor. The current async line requires Ruby >= 3.3, and 3.3 is mainstream;
  # we do not contort the dependency graph to shave a Ruby minor.
  spec.required_ruby_version = ">= 3.3"

  spec.add_dependency "async", "~> 2.42"

  # pg: pipeline bindings + setnonblocking(true) + scheduler-aware socket_io.
  # The EFFECTIVE server floor (libpq >= 14 required, >= 17 optional fast path)
  # is enforced at runtime in ServerCaps via PG.library_version, NOT here.
  spec.add_dependency "pg", ">= 1.5", "< 2"

  spec.files = Dir["lib/**/*.rb", "README.md", "DESIGN.md", "LICENSE.txt", "CHANGELOG.md"]
  spec.require_paths = ["lib"]

  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "rspec", "~> 3.13"
  spec.add_development_dependency "ruby-prof", "~> 1.7"
  spec.add_development_dependency "stackprof", "~> 0.2"
end
