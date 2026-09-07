# Rack input for a request body that is still arriving from the client.
#
# A read that outruns the network parks the fiber on a variable the reactor
# resolves once more bytes land, so waiting never blocks the runtime thread and
# never spins. Rack 3 dropped the rewind requirement, so nothing is retained
# after it has been read.
Class.new do
  READ_SIZE = 64 * 1024

  def initialize(chunks)
    @chunks = chunks
    @buffer = +"".b
  end

  def read(length = nil, buffer = nil)
    return read_all(buffer) if length.nil?
    raise ArgumentError, "negative length #{length}" if length.negative?
    return buffer ? buffer.replace(+"".b) : +"".b if length.zero?

    fill { @buffer.bytesize >= length || @chunks.finished? }
    return nil if @buffer.empty?

    take(length, buffer)
  end

  def gets(separator = $INPUT_RECORD_SEPARATOR)
    return read_all(nil) if separator.nil?

    fill { @buffer.include?(separator) || @chunks.finished? }
    return nil if @buffer.empty?

    index = @buffer.index(separator)
    take(index ? index + separator.bytesize : @buffer.bytesize, nil)
  end

  def each
    while (chunk = read(READ_SIZE))
      yield chunk
    end
  end

  def close
    nil
  end

  private

  def read_all(buffer)
    fill { @chunks.finished? }
    take(@buffer.bytesize, buffer)
  end

  def take(length, buffer)
    result = @buffer.byteslice(0, length)
    @buffer = @buffer.byteslice(length..) || +"".b
    buffer ? buffer.replace(result) : result
  end

  def fill
    loop do
      while (chunk = @chunks.next_chunk)
        @buffer << chunk
      end
      break if yield

      variable = Async::Variable.new
      @chunks.park(variable)
      # A chunk that landed between the drain above and the park found nobody
      # waiting, so look again rather than sleep on a wakeup already delivered.
      variable.wait unless @chunks.ready?
    end
  end
end
