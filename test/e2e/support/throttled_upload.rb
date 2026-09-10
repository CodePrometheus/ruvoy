# frozen_string_literal: true

require "stringio"

# A request body fed at a fixed rate, so a client can walk away mid-upload.
class ThrottledUpload
  def initialize(data, bytes_per_second)
    @io = StringIO.new(data)
    @bytes_per_second = bytes_per_second
    @started = nil
    @sent = 0
  end

  def read(length = nil, outbuf = nil)
    @started ||= clock
    chunk = @io.read(length, outbuf)
    return chunk if chunk.nil?

    @sent += chunk.bytesize
    ahead = @sent / @bytes_per_second.to_f - (clock - @started)
    sleep ahead if ahead.positive?
    chunk
  end

  private

  def clock
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end
end
