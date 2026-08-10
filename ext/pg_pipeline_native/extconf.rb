# frozen_string_literal: true

require "mkmf"
require "open3"

def command_output(*command)
  output, status = Open3.capture2e(*command)
  return unless status.success?

  output.strip
rescue Errno::ENOENT
  nil
end

def configure_from_pg_config(pg_config)
  return false if pg_config.nil? || pg_config.empty?
  return false unless File.file?(pg_config) && File.executable?(pg_config)

  includedir = command_output(pg_config, "--includedir")
  libdir = command_output(pg_config, "--libdir")
  return false if includedir.nil? || includedir.empty? || libdir.nil? || libdir.empty?

  $stderr.puts "Using libpq configuration from #{pg_config}"
  # mkmf quotes these itself; pre-escaping produced backslashes inside the
  # generated Makefile on any path containing a space.
  append_cppflags("-I#{includedir}")
  append_ldflags("-L#{libdir}")
  true
end

def homebrew_pg_config
  brew = find_executable("brew")
  return unless brew

  formulas = %w[libpq libpq@18 libpq@17 libpq@16 postgresql@18 postgresql@17 postgresql@16 postgresql@15 postgresql@14 postgresql]

  formulas.each do |formula|
    prefix = command_output(brew, "--prefix", formula)
    next if prefix.nil? || prefix.empty?

    pg_config = File.join(prefix, "bin", "pg_config")
    return pg_config if File.file?(pg_config) && File.executable?(pg_config)
  end

  nil
end

def libpq_install_help
  <<~HELP

    libpq development files were not found.

    macOS with Homebrew:
      brew install libpq
      export PG_CONFIG="$(brew --prefix libpq)/bin/pg_config"
      bundle config set --local build.pg_pipeline --with-pg-config="$PG_CONFIG"
      bundle install
      PG_CONFIG="$PG_CONFIG" bundle exec rake clean compile

    Debian/Ubuntu:
      sudo apt-get install libpq-dev

    Fedora/RHEL:
      sudo dnf install libpq-devel

    The precompiled pg gem can remain installed. It supplies ruby-pg's runtime,
    but it does not include the development headers needed to compile the
    pg_pipeline extension. Installing Homebrew libpq provides those headers.
  HELP
end

dir_config("libpq")

requested_pg_config = with_config("pg-config") || with_config("pg_config") || ENV["PG_CONFIG"]
configured = false

if requested_pg_config
  unless requested_pg_config.is_a?(String) && !requested_pg_config.empty?
    abort "--with-pg-config requires a path to an executable pg_config"
  end

  resolved_pg_config = if requested_pg_config.include?(File::SEPARATOR)
                         requested_pg_config
                       else
                         find_executable(requested_pg_config)
                       end

  unless resolved_pg_config && configure_from_pg_config(resolved_pg_config)
    abort "PG_CONFIG does not point to an executable pg_config: #{requested_pg_config.inspect}"
  end

  configured = true
else
  configured = pkg_config("libpq")
end

unless configured
  pg_configs = [find_executable("pg_config"), homebrew_pg_config].compact.uniq
  configured = pg_configs.any? { |pg_config| configure_from_pg_config(pg_config) }
end

abort libpq_install_help unless have_header("libpq-fe.h")
abort "libpq was not found#{libpq_install_help}" unless have_library("pq", "PQconnectStart")
abort "libpq pipeline mode requires PQenterPipelineMode (libpq >= 14)" unless have_func("PQenterPipelineMode", "libpq-fe.h")
abort "libpq pipeline mode requires PQpipelineSync (libpq >= 14)" unless have_func("PQpipelineSync", "libpq-fe.h")

have_func("PQsendPipelineSync", "libpq-fe.h")

# gnu99, not c99: Ruby's headers and libpq both expect the GNU feature set, and
# strict c99 hides POSIX declarations on glibc depending on what ruby/config.h
# happens to define first.
$CFLAGS = "#{$CFLAGS} -std=gnu99 -Wall -Wextra -Wno-unused-parameter"
$CFLAGS = "#{$CFLAGS} -Werror" if ENV["PG_PIPELINE_STRICT_BUILD"] == "1"

# Sorted so the object list is identical on every platform and in every build.
$srcs = Dir[File.join(__dir__, "*.c")].map { |path| File.basename(path) }.sort
$objs = $srcs.map { |src| src.sub(/\.c\z/, ".#{$OBJEXT}") }

create_makefile("pg_pipeline/pg_pipeline_native")
