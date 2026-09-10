# Calls a cluster Envoy was configured with, from inside a request.
#
# The call is queued for the worker that owns the request, the only thread
# allowed to reach Envoy, and the fiber parks until that worker has the answer,
# so the reactor keeps serving other requests in the meantime.
Ruvoy::Upstream.class_eval do
  def call(cluster, method, path, headers: {}, body: nil, timeout: nil)
    body = String(body) unless body.nil?
    pending = dispatch(cluster.to_s, fields(cluster, method, path, headers, body), body, timeout)
    loop do
      response = pending.response
      return response if response

      variable = Async::Variable.new
      # Refused when the answer landed after the look above: look again instead.
      next unless pending.park(variable)

      begin
        variable.wait
      ensure
        # A stopped fiber must not leave the reactor a variable nobody holds.
        pending.unpark
      end
    end
  end

  private

  # The pseudo-headers come from the arguments, `host` defaults to the cluster
  # name, and a body is sent with its length rather than chunked.
  def fields(cluster, method, path, headers, body)
    raise ArgumentError, "method must not be empty" if method.to_s.empty?
    raise ArgumentError, "path must start with /, got #{path.inspect}" unless path.to_s.start_with?("/")

    host = cluster.to_s
    fields = [ [ ":method", method.to_s.upcase ], [ ":path", path.to_s ] ]
    headers.each do |name, value|
      name = name.to_s.downcase
      raise ArgumentError, "#{name} is set from the call's arguments, not its headers" if name.start_with?(":")

      if name == "host"
        host = value.to_s
      else
        Array(value).each { |element| fields << [ name, element.to_s ] }
      end
    end
    fields << [ "content-length", body.bytesize.to_s ] if body && fields.none? { |name, _| name == "content-length" }
    fields << [ "host", host ]
  end
end
