# frozen_string_literal: true

# Inject RTT in front of PostgreSQL so localhost benchmarks become realistic.
# Full client↔server round trip costs ~RTT_MS (half delay each direction).
#
#   bundle exec rake bench:proxy RTT_MS=10 UPSTREAM=127.0.0.1:5417 LISTEN=127.0.0.1:6432
#   # then point PG_PIPELINE_URL at the LISTEN port

require "socket"

listen = ENV.fetch("LISTEN", "127.0.0.1:6432")
upstream = ENV.fetch("UPSTREAM", "127.0.0.1:5432")
half = Float(ENV.fetch("RTT_MS", "10")) / 1000.0 / 2.0

lhost, lport = listen.split(":", 2)
uhost, uport = upstream.split(":", 2)
lport = Integer(lport)
uport = Integer(uport)

def pump(from, to, delay)
  while (chunk = from.readpartial(65_536))
    sleep(delay) if delay.positive?
    to.write(chunk)
    to.flush
  end
rescue EOFError, IOError
  nil
ensure
  begin
    to.close_write
  rescue StandardError
    nil
  end
end

server = TCPServer.new(lhost, lport)
warn "latency_proxy: #{listen} -> #{upstream}, RTT=#{(half * 2 * 1000).round(2)}ms"
loop do
  client = server.accept
  Thread.new(client) do |cl|
    up = TCPSocket.new(uhost, uport)
    a = Thread.new { pump(cl, up, half) }
    b = Thread.new { pump(up, cl, half) }
    a.join
    b.join
  rescue StandardError => e
    warn "proxy conn error: #{e.class}: #{e.message}"
  ensure
    begin
      cl.close
    rescue StandardError
      nil
    end
  end
end
