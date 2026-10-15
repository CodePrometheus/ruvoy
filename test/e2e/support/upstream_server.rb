# frozen_string_literal: true

require "digest"
require "json"
require "protocol/websocket"
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
#   /digest         the size, SHA-256 and framing of the request body, as JSON
#   /chunks/N/MS    N chunks of "chunk I\n", MS milliseconds apart
#   /flood/N        N bytes, chunked, as fast as the connection takes them
#   /trailers       a chunked body that ends with a trailer
#   /hang/KEY       one chunk, then nothing until the client hangs up on KEY
#   /ws/echo        a WebSocket that sends every message back
#   /ws/hello       a WebSocket that sends one message and closes
#   /ws/silent/KEY  a WebSocket that sends nothing until the client hangs up on KEY
#   /ws/flood/KEY   a WebSocket that sends 64 KiB frames until the client hangs up on KEY
class UpstreamServer
  WEBSOCKET_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

  attr_reader :port

  def initialize
    @server = TCPServer.new("127.0.0.1", 0)
    @port = @server.addr[1]
    @hits = Hash.new(0)
    @hung_up = []
    @lock = Mutex.new
    @connections = []
  end

  def start
    @acceptor = Thread.new do
      loop do
        socket = @server.accept
        # As a real server would: a frame's header and payload go out together.
        socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
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

  # Whether the client hung up on /hang/KEY.
  def hung_up?(key)
    @lock.synchronize { @hung_up.include?(key) }
  end

  private

  def serve(connection)
    while (request = read_request(connection))
      @lock.synchronize { @hits[request[:path]] += 1 }
      break connection.close if request[:path] == "/reset"
      break if stream(connection, request)

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
      while (size = (connection.gets("\r\n") or raise EOFError).to_i(16)).positive?
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
    when "/ws/echo"
      [ 400, {}, "a WebSocket handshake needs a key" ]
    when "/digest"
      framing = request[:headers]["transfer-encoding"].include?("chunked") ? "chunked" : "length"
      digest = { bytes: request[:body].bytesize, sha256: Digest::SHA256.hexdigest(request[:body]), framing: framing }
      [ 200, { "content-type" => [ "application/json" ] }, JSON.generate(digest) ]
    else
      [ 404, {}, "no route #{request[:path]}" ]
    end
  end

  # Answers the paths whose body is written a piece at a time, on a connection
  # that closes after. False for any other path.
  def stream(connection, request)
    case request[:path]
    when %r{\A/chunks/(\d+)/(\d+)\z}
      count = Integer($1, 10)
      interval = Integer($2, 10) / 1000.0
      chunked(connection) do |send|
        count.times do |index|
          sleep interval if index.positive?
          send.call("chunk #{index}\n")
        end
      end
    when %r{\A/flood/(\d+)\z}
      remaining = Integer($1, 10)
      chunked(connection) do |send|
        while remaining.positive?
          send.call("f" * [ remaining, 16_384 ].min)
          remaining -= 16_384
        end
      end
    when "/trailers"
      chunked(connection, trailer: "x-checksum: 5d41402a\r\n") { |send| send.call("hello") }
    when "/ws/echo"
      return false unless accept_websocket(connection, request)

      echo_frames(Protocol::WebSocket::Framer.new(connection))
    when "/ws/hello"
      accept_websocket(connection, request)
      framer = Protocol::WebSocket::Framer.new(connection)
      framer.write_frame(Protocol::WebSocket::TextFrame.new(true, "hello"))
      framer.write_frame(Protocol::WebSocket::CloseFrame.new(true, [ 1000 ].pack("n")))
    when %r{\A/ws/flood/(.+)\z}
      key = $1
      accept_websocket(connection, request)
      framer = Protocol::WebSocket::Framer.new(connection)
      begin
        loop { framer.write_frame(Protocol::WebSocket::BinaryFrame.new(true, "f" * 65_536)) }
      rescue IOError, SystemCallError
        @lock.synchronize { @hung_up << key }
      end
    when %r{\A/ws/silent/(.+)\z}
      key = $1
      accept_websocket(connection, request)
      connection.read rescue nil
      @lock.synchronize { @hung_up << key }
    when %r{\A/hang/(.+)\z}
      key = $1
      chunked(connection, finish: false) { |send| send.call("first\n") }
      # Nothing more arrives on the connection until the client closes it.
      connection.read rescue nil
      @lock.synchronize { @hung_up << key }
    else
      return false
    end
    true
  end

  # Answers the handshake, or refuses one without a key with a 400 the
  # caller sees as an ordinary response.
  def accept_websocket(connection, request)
    key = request[:headers]["sec-websocket-key"].first
    return false if key.nil?

    accept = Digest::SHA1.base64digest(key + WEBSOCKET_GUID)
    connection.write("HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\nconnection: Upgrade\r\n" \
                     "sec-websocket-accept: #{accept}\r\n\r\n")
  end

  def echo_frames(framer)
    loop do
      frame = framer.read_frame
      case frame
      when Protocol::WebSocket::PingFrame
        framer.write_frame(Protocol::WebSocket::PongFrame.new(true, frame.unpack))
      when Protocol::WebSocket::CloseFrame
        break framer.write_frame(Protocol::WebSocket::CloseFrame.new.pack(*frame.unpack))
      else
        framer.write_frame(frame.class.new(frame.finished?, frame.unpack))
      end
    end
  rescue EOFError, Protocol::WebSocket::ProtocolError
    nil
  end

  def chunked(connection, finish: true, trailer: nil)
    connection.write("HTTP/1.1 200 Streaming\r\ncontent-type: text/plain\r\ntransfer-encoding: chunked\r\n" \
                     "connection: close\r\n\r\n")
    yield ->(data) { connection.write("#{data.bytesize.to_s(16)}\r\n", data, "\r\n") }
    connection.write("0\r\n#{trailer}\r\n") if finish
  end

  def write_response(connection, status, headers, body)
    head = +"HTTP/1.1 #{status} Scripted\r\ncontent-length: #{body.bytesize}\r\n"
    headers.each { |name, values| values.each { |value| head << "#{name}: #{value}\r\n" } }
    connection.write(head, "\r\n", body)
  end
end
