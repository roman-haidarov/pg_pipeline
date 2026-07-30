# frozen_string_literal: true

# No PostgreSQL needed. Shows why pipelining only wins when RTT > 0:
# serial pays N full RTTs; pipelined amortizes to ~1 RTT + serial service.
#
#   bundle exec rake bench:rtt_demo
#   N=64 RTTS=0,2,10,30 bundle exec rake bench:rtt_demo

require "socket"
require_relative "common"

def now = BenchKit.now

def start_server(service)
  srv = TCPServer.new("127.0.0.1", 0)
  Thread.new do
    loop do
      conn = srv.accept
      Thread.new(conn) do |c|
        while (line = c.gets("\n"))
          sleep(service) if service.positive?
          c.write("r:#{line}")
          c.flush
        end
      rescue StandardError
        nil
      ensure
        begin
          c.close
        rescue StandardError
          nil
        end
      end
    end
  end
  srv.addr[1]
end

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

def start_proxy(upstream_port, half_rtt)
  prox = TCPServer.new("127.0.0.1", 0)
  Thread.new do
    loop do
      client = prox.accept
      Thread.new(client) do |cl|
        up = TCPSocket.new("127.0.0.1", upstream_port)
        a = Thread.new { pump(cl, up, half_rtt) }
        b = Thread.new { pump(up, cl, half_rtt) }
        a.join
        b.join
      rescue StandardError
        nil
      ensure
        begin
          cl.close
        rescue StandardError
          nil
        end
      end
    end
  end
  prox.addr[1]
end

def run_serial(port, n)
  s = TCPSocket.new("127.0.0.1", port)
  n.times do |i|
    s.write("q#{i}\n")
    s.flush
    s.gets("\n")
  end
  s.close
end

def run_pipelined(port, n)
  s = TCPSocket.new("127.0.0.1", port)
  s.write((0...n).map { |i| "q#{i}\n" }.join)
  s.flush
  n.times { s.gets("\n") }
  s.close
end

def measure(port, mode, n, reps)
  best = nil
  reps.times do
    t = now
    mode == :serial ? run_serial(port, n) : run_pipelined(port, n)
    dt = now - t
    best = dt if best.nil? || dt < best
  end
  best
end

n = Integer(ENV.fetch("N", "64"))
service_ms = Float(ENV.fetch("SERVICE_MS", "0.5"))
reps = Integer(ENV.fetch("REPS", "3"))
rtts = (ENV["RTTS"] || "0,2,10,30").split(",").map(&:to_f)

puts "Real-socket round-trip amortization demo"
puts "N=#{n} requests/conn, server service=#{service_ms}ms/req (serial like one PG backend), best of #{reps}"
puts
printf("%8s | %12s | %12s | %8s | %s\n", "RTT ms", "serial ms", "pipelined ms", "speedup", "note")
puts "-" * 78

up = start_server(service_ms / 1000.0)

rtts.each do |rtt_ms|
  port = start_proxy(up, (rtt_ms / 1000.0) / 2.0)
  sleep 0.05
  run_pipelined(port, 4)
  serial = measure(port, :serial, n, reps) * 1000.0
  pipel = measure(port, :pipelined, n, reps) * 1000.0
  speed = serial / pipel
  note =
    if rtt_ms.zero?
      "localhost trap: nothing to amortize"
    else
      "saved ~= (N-1) x RTT"
    end
  printf("%8.1f | %12.1f | %12.1f | %7.1fx | %s\n", rtt_ms, serial, pipel, speed, note)
end
