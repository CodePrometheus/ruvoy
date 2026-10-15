require "async/queue"

# Byte streams between a fiber and the worker that owns its request.
module Ruvoy
  # Waiting on a queue without blocking the runtime thread: the fiber parks on a
  # variable the reactor resolves once the other side makes progress.
  module Park
    # Waits until `queue` is woken, at most `timeout` seconds. Returns at once
    # when the queue refuses because there is no reason to wait.
    def self.on(queue, timeout = nil)
      variable = Async::Variable.new
      return unless queue.park(variable)

      begin
        timeout ? Async::Task.current.with_timeout(timeout) { variable.wait } : variable.wait
      ensure
        # A stopped fiber must not leave the reactor a variable nobody holds.
        queue.unpark
      end
    end
  end

  # A duplex byte stream over a chunk source and a chunk sink, with the IO
  # methods a Rack streaming body is called with. A streaming response reads
  # what the client sends and writes what goes back; an upgraded upstream
  # exchange is the same stream facing the other way. Nothing read is kept, so
  # a connection can stay open for as long as both ends want.
  class Stream
    RELAY_BYTES = 64 * 1024

    # Writes `chunk` whole into `sink`, parking while it is full. False once
    # the other side has gone.
    def self.write(sink, chunk)
      offset = 0
      while offset < chunk.bytesize
        Park.on(sink) unless sink.writable?
        taken = sink.write(chunk, offset)
        return false if taken.nil?

        offset += taken
      end
      true
    end

    # Bytes handed to the reader and taken from the writer so far.
    attr_reader :bytes_read, :bytes_written

    def initialize(source, sink, buffered = +"".b)
      @source = source
      @sink = sink
      @buffer = buffered
      @failure = nil
      @read_closed = false
      @write_closed = false
      @bytes_read = 0
      @bytes_written = 0
    end

    def read(length = nil, buffer = nil)
      readable!
      if length.nil?
        fill { finished? }
        raise_failure if @buffer.empty?
        return give(@buffer.bytesize, buffer)
      end
      raise ArgumentError, "negative length #{length}" if length.negative?
      return give(0, buffer) if length.zero?

      fill { @buffer.bytesize >= length || finished? }
      return give([ length, @buffer.bytesize ].min, buffer) unless @buffer.empty?

      raise_failure
      nil
    end

    def readpartial(length, buffer = nil)
      readable!
      fill { !@buffer.empty? || finished? }
      if @buffer.empty?
        raise_failure
        raise EOFError, "end of file reached"
      end

      give([ length, @buffer.bytesize ].min, buffer)
    end

    def write(*strings)
      raise IOError, "not opened for writing" if @write_closed

      strings.sum do |string|
        string = String(string)
        raise Errno::EPIPE, "the other side has gone" unless Stream.write(@sink, string)

        @bytes_written += string.bytesize
        string.bytesize
      end
    end

    def <<(string)
      write(string)
      self
    end

    def flush
      self
    end

    def close_read
      @read_closed = true
      nil
    end

    # The other side sees the end of the stream once what was written drains.
    def close_write
      return if @write_closed

      @write_closed = true
      @sink.finish
      nil
    end

    def close(_error = nil)
      close_read
      close_write
    end

    def closed?
      @read_closed && @write_closed
    end

    # Copies what each side sends to the other. The end of one direction
    # closes the write side it was feeding, as a TCP proxy would; once both
    # directions have ended, both streams are closed. A failure ends the relay
    # with it and leaves both open, for whoever owns each to reset it.
    #
    # `other` must read like a socket, with readpartial: Rack::Lint's stream
    # wrapper offers only the methods the specification requires.
    def relay(other)
      raise ArgumentError, "a relay reads with readpartial, which #{other.class} lacks" unless other.respond_to?(:readpartial)

      # Each direction reports how it ended; the first failure ends the relay
      # at once, however quiet the other direction is.
      endings = Async::Queue.new
      directions = [ [ self, other ], [ other, self ] ].map do |from, to|
        Async do
          copy(from, to)
          endings.push(nil)
        rescue StandardError => error
          endings.push(error)
        end
      end
      2.times do
        failure = endings.pop
        raise failure if failure
      end
      other.close
      close
    ensure
      directions&.each(&:stop)
    end

    private

    def copy(from, to)
      loop { to.write(from.readpartial(RELAY_BYTES)) }
    rescue EOFError
      to.close_write
    rescue Errno::EPIPE
      # Nobody is left to write to, so there is nothing more to read for.
      from.close_read
    end

    # Parks until more arrives; a stream facing an upstream bounds the wait.
    def wait_for_input
      Park.on(@source)
    end

    def readable!
      raise IOError, "not opened for reading" if @read_closed
    end

    def finished?
      !@failure.nil? || @source.finished?
    end

    # What arrived before a failure is read first; the failure surfaces once
    # nothing is left.
    def raise_failure
      raise @failure if @failure
    end

    def fill
      loop do
        drain
        break if yield

        wait_for_input
      end
    end

    def drain
      while @failure.nil? && (chunk = @source.next_chunk)
        @buffer << chunk
      end
    rescue StandardError => error
      @failure = error
    end

    def give(length, buffer)
      data = @buffer.byteslice(0, length)
      @buffer = @buffer.byteslice(length..) || +"".b
      @bytes_read += data.bytesize
      buffer ? buffer.replace(data) : data
    end
  end
end
