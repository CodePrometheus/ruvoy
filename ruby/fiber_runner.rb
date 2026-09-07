# Drives the reactor that runs one fiber per request.
#
# The bridge file descriptor becomes readable whenever Rust submits work, so the
# reactor can wait on it like any other source of I/O.
#
# The wait has a deadline so the loop comes back around even with nothing to do.
# That turns "the loop stopped running" into an unambiguous signal that some
# fiber is blocking the thread rather than yielding.
lambda do |bridge, app, rack_errors, body_reader|
  io = IO.for_fd(bridge.fd, autoclose: false)
  heartbeat_seconds = 0.1

  begin
    Async do |parent|
      shutting_down = false
      until shutting_down
        io.wait_readable(heartbeat_seconds)
        envelopes, shutting_down = bridge.drain
        envelopes.each do |envelope|
          parent.async(envelope) do |_task, current_envelope|
            bridge.execute(current_envelope, app, rack_errors, body_reader)
          end
        end
      end
      parent.wait_all
    end
  ensure
    bridge.ack_shutdown
  end
end
