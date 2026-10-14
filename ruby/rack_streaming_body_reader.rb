# Pushes a Rack response body into the sink one chunk at a time.
#
# A full queue parks the fiber on a variable the reactor resolves once the
# worker has drained room or the client has gone, so waiting never blocks the
# runtime thread and nothing polls.
lambda do |body, sink|
  raise TypeError, "streaming Rack response bodies are not supported" unless body.respond_to?(:each)

  wait_for_room = lambda do
    variable = Async::Variable.new
    # Refused when room opened up after the check, or the client has gone.
    return unless sink.park(variable)

    begin
      variable.wait
    ensure
      # A stopped fiber must not leave the reactor a variable nobody holds.
      sink.unpark
    end
  end

  begin
    body.each do |chunk|
      offset = 0
      # The sink takes no more than the queue has room for, so a chunk larger
      # than the budget goes over in slices.
      while offset < chunk.bytesize
        wait_for_room.call unless sink.writable?
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
