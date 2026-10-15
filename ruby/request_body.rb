require "tempfile"

# Rack input for a request body that is still arriving from the client.
#
# A read that outruns the network parks the fiber on a variable the reactor
# resolves once more bytes land, so waiting never blocks the runtime thread and
# never spins. Every byte pulled from the client is kept, in memory up to a
# limit and in a temporary file beyond it, so the input can be rewound and read
# again the way frameworks written for earlier Rack versions expect.
Class.new do
  READ_SIZE = 64 * 1024
  KEPT_IN_MEMORY = 128 * 1024

  def initialize(chunks)
    @chunks = chunks
    @kept = +"".b
    @file = nil
    @size = 0
    @position = 0
  end

  def read(length = nil, buffer = nil)
    return read_all(buffer) if length.nil?
    raise ArgumentError, "negative length #{length}" if length.negative?
    return buffer ? buffer.replace(+"".b) : +"".b if length.zero?

    fill { unread >= length || @chunks.finished? }
    return nil if unread.zero?

    take([ length, unread ].min, buffer)
  end

  def gets(separator = $/)
    if separator.nil?
      fill { @chunks.finished? }
      return unread.zero? ? nil : take(unread, nil)
    end

    from = @position
    found = nil
    fill do
      found = search(separator, from)
      from = [ @size - separator.bytesize + 1, @position ].max
      found || @chunks.finished?
    end
    return nil if unread.zero?

    take(found ? found + separator.bytesize - @position : unread, nil)
  end

  def each
    while (chunk = read(READ_SIZE))
      yield chunk
    end
  end

  def rewind
    @position = 0
  end

  def close
    @file&.close
    nil
  end

  # Hands the rest of the body to a reader that keeps nothing, for a connection
  # upgraded to another protocol, which stays open for as long as the client
  # does. Returns the bytes pulled but not yet read, and the chunks still to
  # come; the input cannot be read afterwards.
  def detach
    leftover = unread.zero? ? +"".b : take(unread, nil)
    chunks = @chunks
    @chunks = nil
    close
    [ leftover, chunks ]
  end

  private

  def unread
    @size - @position
  end

  def read_all(buffer)
    fill { @chunks.finished? }
    take(unread, buffer)
  end

  def take(length, buffer)
    result = peek(@position, length)
    @position += result.bytesize
    buffer ? buffer.replace(result) : result
  end

  def peek(offset, length)
    length = [ length, @size - offset ].min
    return +"".b unless length.positive?

    @file ? @file.pread(length, offset) : @kept.byteslice(offset, length)
  end

  # Where the separator first starts at or after `from`, among the bytes kept so far.
  def search(separator, from)
    while from < @size
      index = peek(from, READ_SIZE + separator.bytesize - 1).index(separator)
      return from + index if index

      from += READ_SIZE
    end
    nil
  end

  def keep(chunk)
    if @file.nil? && @size + chunk.bytesize > KEPT_IN_MEMORY
      @file = Tempfile.create("ruvoy-input", anonymous: true)
      # Bytes as they came, whatever Encoding.default_internal the application set.
      @file.binmode
      @file.write(@kept)
      @kept = nil
    end
    @file ? @file.write(chunk) : @kept << chunk
    @size += chunk.bytesize
  end

  def fill
    raise IOError, "rack.input was handed to the response stream" if @chunks.nil?

    loop do
      while (chunk = @chunks.next_chunk)
        keep(chunk)
      end
      break if yield

      Ruvoy::Park.on(@chunks)
    end
  end
end
