require "securerandom"

# Calls a cluster Envoy was configured with, from inside a request.
#
# The call is queued for the worker that owns the request, the only thread
# allowed to reach Envoy, and the fiber parks until that worker has the answer,
# so the reactor keeps serving other requests in the meantime.
Ruvoy::Upstream.class_eval do
  def call(cluster, method, path, headers: {}, body: nil, timeout: nil)
    body = String(body) unless body.nil?
    pending = dispatch(cluster.to_s, fields(cluster, method, path, headers, body), body, timeout)
    loop do
      response = pending.response
      return response if response

      Ruvoy::Park.on(pending)
    end
  end

  # Streams a call: returns once the response head is in, with a body that
  # yields the response as it arrives and can be returned as a Rack body.
  # `idle_timeout` bounds every wait on the upstream; `timeout`, if given, the
  # whole exchange. The body may be a String or anything that responds to read,
  # such as `rack.input`.
  #
  # With `protocol`, the request asks the upstream to upgrade to it. Once the
  # upstream agrees with a 101, the body is a Ruvoy::UpstreamStream that reads
  # what the upstream sends and writes what goes back, and the response,
  # returned as is, upgrades the client's connection too and relays between
  # the two. A refusal arrives as an ordinary response. A WebSocket handshake
  # without a key, as from an HTTP/2 client, is given one.
  def stream(cluster, method, path, headers: {}, body: nil, idle_timeout: nil, timeout: nil, protocol: nil)
    idle_timeout ||= default_timeout
    unless idle_timeout.is_a?(Numeric) && idle_timeout.positive?
      raise ArgumentError, "idle_timeout must be a positive number of seconds, got #{idle_timeout.inspect}"
    end
    unless body.nil? || body.is_a?(String) || body.respond_to?(:read)
      raise ArgumentError, "body must be a String or respond to read"
    end
    if protocol
      raise ArgumentError, "an upgraded stream sends its body through the stream it returns" unless body.nil?

      headers = headers.reject { |name, _| %w[upgrade connection].include?(name.to_s.downcase) }
      headers = headers.merge("upgrade" => protocol.to_s, "connection" => "upgrade")
      if protocol.to_s.casecmp?("websocket") && headers.keys.none? { |name| name.to_s.casecmp?("sec-websocket-key") }
        headers = headers.merge("sec-websocket-key" => SecureRandom.base64(16), "sec-websocket-version" => "13")
      end
    end

    # An IO body that ends within its first piece goes with its length, and an
    # empty one as none; a longer one goes up as it is read.
    body, rest = Ruvoy::UpstreamBody.preread(body) unless body.nil? || body.is_a?(String)
    channel = open_stream(cluster.to_s, fields(cluster, method, path, headers, rest ? nil : body), timeout,
                          !protocol.nil? || !body.nil?)
    upload = upload(channel, body, rest) unless body.nil?
    response = Ruvoy::UpstreamBody.new(channel, upload, idle_timeout)
    begin
      status, response_headers = response.head
    rescue Exception
      response.close
      raise
    end
    if protocol && status == 101
      response_headers = response_headers.except("upgrade", "connection").merge("rack.protocol" => protocol.to_s)
      return [ status, response_headers, Ruvoy::UpstreamStream.new(channel, idle_timeout) ]
    end

    # A refused upgrade sends nothing more.
    channel.writer.finish if protocol
    [ status, response_headers, response ]
  end

  private

  # The pseudo-headers come from the arguments, `host` defaults to the cluster
  # name, and a body is sent with its length rather than chunked.
  def fields(cluster, method, path, headers, body)
    raise ArgumentError, "method must not be empty" if method.to_s.empty?
    raise ArgumentError, "path must start with /, got #{path.inspect}" unless path.to_s.start_with?("/")

    host = cluster.to_s
    fields = [ [ ":method", method.to_s.upcase ], [ ":path", path.to_s ] ]
    headers.each do |name, value|
      name = name.to_s.downcase
      raise ArgumentError, "#{name} is set from the call's arguments, not its headers" if name.start_with?(":")

      if name == "host"
        host = value.to_s
      else
        Array(value).each { |element| fields << [ name, element.to_s ] }
      end
    end
    fields << [ "content-length", body.bytesize.to_s ] if body && fields.none? { |name, _| name == "content-length" }
    fields << [ "host", host ]
  end

  # The body goes up in its own fiber, since an upstream may start answering
  # before it has all of it: `read` first, then whatever `rest` still holds.
  def upload(channel, read, rest)
    writer = channel.writer
    Async do
      sent = Ruvoy::Stream.write(writer, read)
      while sent && rest && (chunk = rest.read(Ruvoy::UpstreamBody::UPLOAD_BYTES))
        sent = Ruvoy::Stream.write(writer, chunk)
      end
      writer.finish
    rescue StandardError
      # The reader hears of it as a closed stream.
      channel.close
      raise
    end
  end
end

module Ruvoy
  # Waiting on an upstream that may go quiet: silence past the idle timeout
  # gives the exchange up.
  module UpstreamWaiting
    private

    def await(queue)
      Park.on(queue, @idle_timeout)
    rescue Async::TimeoutError
      @channel.time_out
      raise UpstreamTimeout, "the upstream sent nothing for #{@idle_timeout} seconds"
    end
  end

  # The body of a streamed upstream response. It yields the response as it
  # arrives, stops the stream when closed early, and can be returned as a
  # Rack body.
  class UpstreamBody
    include UpstreamWaiting

    UPLOAD_BYTES = 64 * 1024

    # Reads `io` until it ends or has given more than one upload piece. Returns
    # what was read, or nil when the body is empty, and `io` when more may
    # follow. Only nil marks the end, since a read may return less than asked.
    def self.preread(io)
      read = +"".b
      while read.bytesize <= UPLOAD_BYTES
        piece = io.read(UPLOAD_BYTES) or return [ (read unless read.empty?), nil ]
        read << piece
      end
      [ read, io ]
    end

    def initialize(channel, upload, idle_timeout)
      @channel = channel
      @reader = channel.reader
      @upload = upload
      @idle_timeout = idle_timeout
      @finished = false
      @closed = false
    end

    # `[status, headers]`, once the upstream has answered.
    def head
      loop do
        head = @channel.head
        return head if head

        await(@reader)
      end
    end

    def each
      return enum_for(:each) unless block_given?

      until @finished
        while (chunk = @reader.next_chunk)
          yield chunk
        end
        @finished = @reader.finished?
        await(@reader) unless @finished
      end
    end

    # The upstream's trailers, once the body has been read to its end.
    def trailers
      @channel.trailers
    end

    # Stops a stream that was not read to its end, or whose body is still
    # going up.
    def close
      return if @closed

      @closed = true
      uploading = @upload&.alive?
      @upload&.stop
      @channel.close if uploading || !@finished
    end
  end

  # An upstream exchange upgraded to another protocol: the duplex stream a
  # streaming response is served with, facing the upstream. Silence past the
  # idle timeout gives it up, and closing it resets the exchange.
  class UpstreamStream < Stream
    include UpstreamWaiting

    def initialize(channel, idle_timeout)
      super(channel.reader, channel.writer)
      @channel = channel
      @idle_timeout = idle_timeout
    end

    # As a Rack streaming body: relays between the client and the upstream.
    def call(stream)
      relay(stream)
    end

    def close(error = nil)
      super
      @channel.close
    end

    private

    def wait_for_input
      await(@source)
    end
  end
end
