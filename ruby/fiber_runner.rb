# Drives the reactor that runs one fiber per request.
#
# The bridge file descriptor becomes readable whenever Rust submits work, so the
# reactor can wait on it like any other source of I/O.
#
# The wait has a deadline so the loop comes back around even with nothing to do.
# That turns "the loop stopped running" into an unambiguous signal that some
# fiber is blocking the thread rather than yielding, and it bounds how long a
# request whose client has gone away keeps running.
lambda do |bridge, app, rack_errors, body_reader|
  io = IO.for_fd(bridge.fd, autoclose: false)
  heartbeat_seconds = 0.1
  running = {}

  begin
    Async do |parent|
      shutting_down = false
      until shutting_down
        io.wait_readable(heartbeat_seconds)
        envelopes, shutting_down = bridge.drain
        # Registered from inside the task rather than from its return value:
        # a request that never blocks finishes before `async` returns, so an
        # assignment made out here would land after the task had already left.
        envelopes.each do |envelope|
          parent.async(envelope) do |task, current_envelope|
            running[current_envelope] = task
            bridge.execute(current_envelope, app, rack_errors, body_reader)
          ensure
            running.delete(current_envelope)
          end
        end
        # Stopping the fiber is what frees the admission slot and abandons the
        # work; discarding the response on its own would not.
        running.each { |envelope, task| task.stop if envelope.cancelled? }
      end
      parent.wait_all
    end
  ensure
    bridge.ack_shutdown
  end
end
