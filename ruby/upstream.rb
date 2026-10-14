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

      variable = Async::Variable.new
      # Refused when the answer landed after the look above: look again instead.
      next unless pending.park(variable)

      begin
        variable.wait
      ensure
        # A stopped fiber must not leave the reactor a variable nobody holds.
        pending.unpark
      end
    end
  end

  # Streams a call: returns once the response head is in, with a body that
  # yields the response as it arrives and can be returned as a Rack body.
  # `idle_timeout` bounds every wait on the upstream; `timeout`, if given, the
  # whole exchange. The body may be a String or anything that responds to read,
  # such as `rack.input`.
  def stream(cluster, method, path, headers: {}, body: nil, idle_timeout: nil, timeout: nil)
    idle_timeout ||= default_timeout
    unless idle_timeout.is_a?(Numeric) && idle_timeout.positive?
      raise ArgumentError, "idle_timeout must be a positive number of seconds, got #{idle_timeout.inspect}"
    end
    unless body.nil? || body.is_a?(String) || body.respond_to?(:read)
      raise ArgumentError, "body must be a String or respond to read"
    end

    # An IO body that ends within its first piece goes with its length, and an
    # empty one as none; a longer one goes up as it is read.
    body, rest = Ruvoy::UpstreamBody.preread(body) unless body.nil? || body.is_a?(String)
    channel = open_stream(cluster.to_s, fields(cluster, method, path, headers, rest ? nil : body), timeout,
                          !body.nil?)
    upload = upload(channel, body, rest) unless body.nil?
    begin
      status, response_headers = Ruvoy::UpstreamBody.wait(channel, idle_timeout) { channel.head }
    rescue Exception
      upload&.stop
      channel.close
      raise
    end
    [ status, response_headers, Ruvoy::UpstreamBody.new(channel, upload, idle_timeout) ]
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
    Async do
      Ruvoy::UpstreamBody.write(channel, read)
      while rest && (chunk = rest.read(Ruvoy::UpstreamBody::UPLOAD_BYTES))
        Ruvoy::UpstreamBody.write(channel, chunk)
      end
      channel.finish_body
    rescue StandardError
      # The reader hears of it as a closed stream.
      channel.close
      raise
    end
  end
end

module Ruvoy
  # The body of a streamed upstream response. It yields the response as it
  # arrives, stops the stream when closed early, and can be returned as a
  # Rack body.
  class UpstreamBody
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

    # Waits until the block has something to report, parking the fiber
    # between looks. An upstream silent for `idle_timeout` seconds is given up.
    def self.wait(channel, idle_timeout)
      loop do
        value = yield
        return value unless value.nil?

        variable = Async::Variable.new
        # Refused when something landed after the look above: look again.
        next unless channel.park(variable)

        begin
          Async::Task.current.with_timeout(idle_timeout) { variable.wait }
        rescue Async::TimeoutError
          channel.close
          raise Ruvoy::UpstreamTimeout, "the upstream sent nothing for #{idle_timeout} seconds"
        ensure
          # A stopped fiber must not leave the reactor a variable nobody holds.
          channel.unpark
        end
      end
    end

    # Writes `chunk` whole, parking while the worker has not sent enough of
    # the body to make room.
    def self.write(channel, chunk)
      offset = 0
      while offset < chunk.bytesize
        wait_for_room(channel) unless channel.writable?
        taken = channel.write(chunk, offset)
        return if taken.nil?

        offset += taken
      end
    end

    def self.wait_for_room(channel)
      variable = Async::Variable.new
      return unless channel.park_writer(variable)

      begin
        variable.wait
      ensure
        channel.unpark_writer
      end
    end
    private_class_method :wait_for_room

    def initialize(channel, upload, idle_timeout)
      @channel = channel
      @upload = upload
      @idle_timeout = idle_timeout
      @finished = false
      @closed = false
    end

    def each
      return enum_for(:each) unless block_given?

      until @finished
        chunk = self.class.wait(@channel, @idle_timeout) { @channel.read }
        if chunk == false
          @finished = true
        else
          yield chunk
        end
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
end
