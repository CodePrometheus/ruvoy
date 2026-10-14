# Pushes a Rack response body into the sink one chunk at a time.
#
# Waiting for capacity happens here because only Ruby can yield the fiber;
# blocking in Rust would stall every other request on the runtime thread.
lambda do |body, sink|
  raise TypeError, "streaming Rack response bodies are not supported" unless body.respond_to?(:each)

  begin
    body.each do |chunk|
      offset = 0
      # The sink takes no more than the queue has room for, so a chunk larger
      # than the budget goes over in slices.
      while offset < chunk.bytesize
        until sink.writable?
          return if sink.cancelled?

          sleep 0.001
        end
        taken = sink.write(chunk, offset)
        return if taken.nil?

        offset += taken
      end
    end
  ensure
    body.close if body.respond_to?(:close)
  end
  nil
end
