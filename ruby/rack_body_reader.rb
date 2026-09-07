# Collects a Rack response body into a single binary string.
lambda do |body|
  raise TypeError, "streaming Rack response bodies are not supported" unless body.respond_to?(:each)

  output = +"".b
  begin
    body.each { |chunk| output << chunk }
  ensure
    body.close if body.respond_to?(:close)
  end
  output
end
