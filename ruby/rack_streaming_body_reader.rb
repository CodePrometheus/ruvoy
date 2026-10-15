# Feeds a Rack response body to the client: an enumerable body a chunk at a
# time, a streaming body through a duplex stream that also reads what the
# client sends, which is how a connection upgraded through rack.protocol is
# served. A full queue parks the fiber instead of blocking the runtime thread.
lambda do |body, sink, input|
  if body.respond_to?(:each)
    body.each { |chunk| return unless Ruvoy::Stream.write(sink, chunk) }
  elsif body.respond_to?(:call)
    buffered, source = input.detach
    stream = Ruvoy::Stream.new(source, sink, buffered)
    body.call(stream)
    # Only a body that returned ends cleanly; one that raised is reset.
    stream.close_write
  else
    raise TypeError, "a Rack response body must respond to each or call"
  end
  nil
ensure
  body.close if body.respond_to?(:close)
end
