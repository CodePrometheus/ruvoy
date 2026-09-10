# frozen_string_literal: true

require "json"
require "socket"

# The upstream an application reaches through Envoy: a small HTTP/1.1 server in
# the test process, so a suite can script it and see what arrived.
#
#   /echo           what the request looked like, as JSON
#   /status/CODE    that status
#   /delay/MS       200 after MS milliseconds
#   /bytes/N        N bytes of body
#   /flaky/KEY      503 the first time KEY is asked for, 200 after that
#   /reset          closes the connection without answering
class UpstreamServer
  attr_reader :port

  def initialize
    @server = TCPServer.new("127.0.0.1", 0)
    @port = @server.addr[1]
    @hits = Hash.new(0)
    @lock = Mutex.new
    @connections = []
  end

  def start
    @acceptor = Thread.new do
      loop do
        socket = @server.accept
        @lock.synchronize { @connections << socket }
        Thread.new(socket) { |connection| serve(connection) }
      end
    rescue IOError, Errno::EBADF
      nil
    end
    self
  end

  def stop
    @server.close
    @acceptor&.join(1)
    @lock.synchronize { @connections.each { |connection| connection.close rescue nil } }
  end

  # How many requests reached `path`.
  def hits(path)
    @lock.synchronize { @hits[path] }
  end

  private

  def serve(connection)
    while (request = read_request(connection))
      @lock.synchronize { @hits[request[:path]] += 1 }
      break connection.close if request[:path] == "/reset"

      status, headers, body = respond(request)
      write_response(connection, status, headers, body)
    end
  rescue IOError, SystemCallError
    nil
  ensure
    connection.close rescue nil
  end

  def read_request(connection)
    line = connection.gets("\r\n") or return nil
    method, target = line.split(" ", 3)
    headers = Hash.new { |hash, name| hash[name] = [] }
    while (field = connection.gets("\r\n")) && field != "\r\n"
      name, value = field.chomp("\r\n").split(":", 2)
      headers[name.downcase] << value.strip
    end
    { method: method, path: target.split("?", 2).first, target: target, headers: headers,
      body: read_body(connection, headers) }
  end

  def read_body(connection, headers)
    if headers["transfer-encoding"].include?("chunked")
      body = +"".b
      while (size = connection.gets("\r\n").to_i(16)).positive?
        body << connection.read(size)
        connection.read(2)
      end
      connection.read(2)
      body
    else
      connection.read(Integer(headers["content-length"].first || "0", 10)) || ""
    end
  end

  def respond(request)
    case request[:path]
    when "/echo"
      echo = request.slice(:method, :target, :headers, :body)
      [ 200, { "content-type" => [ "application/json" ], "set-cookie" => %w[a=1 b=2] }, JSON.generate(echo) ]
    when %r{\A/status/(\d{3})\z}
      [ Integer($1, 10), {}, "status #{$1}" ]
    when %r{\A/delay/(\d+)\z}
      sleep Integer($1, 10) / 1000.0
      [ 200, {}, "delayed" ]
    when %r{\A/bytes/(\d+)\z}
      [ 200, {}, "u" * Integer($1, 10) ]
    when %r{\A/flaky/}
      hits(request[:path]) == 1 ? [ 503, {}, "not yet" ] : [ 200, {}, "recovered" ]
    else
      [ 404, {}, "no route #{request[:path]}" ]
    end
  end

  def write_response(connection, status, headers, body)
    head = +"HTTP/1.1 #{status} Scripted\r\ncontent-length: #{body.bytesize}\r\n"
    headers.each { |name, values| values.each { |value| head << "#{name}: #{value}\r\n" } }
    connection.write(head, "\r\n", body)
  end
end
