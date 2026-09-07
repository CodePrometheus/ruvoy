# Pushes a Rack response body into the sink one chunk at a time.
#
# Waiting for capacity happens here because only Ruby can yield the fiber;
# blocking in Rust would stall every other request on the runtime thread.
lambda do |body, sink|
  raise TypeError, "streaming Rack response bodies are not supported" unless body.respond_to?(:each)

  begin
    body.each do |chunk|
      until sink.writable?
        break if sink.cancelled?

        sleep 0.001
      end
      break unless sink.write(chunk)
    end
  ensure
    body.close if body.respond_to?(:close)
  end
  nil
end
