# frozen_string_literal: true

require_relative "lib/pg_pipeline/version"

Gem::Specification.new do |spec|
  spec.name = "pg_pipeline"
  spec.version = PgPipeline::VERSION
  spec.authors = ["Roman Hajdarov"]
  spec.email   = ["romanhajdarov@gmail.com"]
  spec.summary = "Fiber-scheduler PostgreSQL pipeline multiplexing with a native libpq core"
  spec.description = <<~DESC
    A driver-adjacent control plane over libpq. It multiplexes independent,
    session-neutral extended-protocol operations from many fibers onto a small
    number of PostgreSQL connections while keeping explicit transactions and
    session-changing work on exclusive pinned connections. Requires any
    Fiber::Scheduler host (Async::Scheduler, Itsi::Scheduler, …). Version 0.4
    moves the multiplexed data plane (request state, PGresult ownership,
    send/flush/drain) into a C extension; behaviour matches 0.3.1.
  DESC
  spec.homepage = "https://github.com/roman-haidarov/pg_pipeline"
  spec.license  = "MIT"
  spec.metadata = {
    "homepage_uri"     => "https://github.com/roman-haidarov/pg_pipeline",
    "source_code_uri"  => "https://github.com/roman-haidarov/pg_pipeline/tree/main",
    "changelog_uri"    => "https://github.com/roman-haidarov/pg_pipeline/blob/main/CHANGELOG.md",
    "bug_tracker_uri"  => "https://github.com/roman-haidarov/pg_pipeline/issues"
  }

  spec.required_ruby_version = ">= 3.3"

  spec.add_dependency "pg", ">= 1.5", "< 2"

  spec.files = Dir[
    "lib/**/*.rb",
    "ext/**/*.{c,h,rb}",
    "README.md",
    "DESIGN.md",
    "LICENSE.txt",
    "CHANGELOG.md",
    "docs/**/*.md"
  ]
  spec.require_paths = ["lib"]
  spec.extensions = ["ext/pg_pipeline_native/extconf.rb"]

  spec.add_development_dependency "async", "~> 2.42"
  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "rspec", "~> 3.13"
  spec.add_development_dependency "ruby-prof", "~> 1.7"
  spec.add_development_dependency "stackprof", "~> 0.2"
end
