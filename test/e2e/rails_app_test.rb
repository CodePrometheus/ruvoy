# frozen_string_literal: true

require "json"
require "securerandom"
require_relative "test_helper"

# A production-mode Rails application on Ruvoy, judged against the same
# application on a reference server.
#
# Nothing here hand-writes an expected answer. Every request goes to both
# servers, each pass starting from the same seed rows, and the answers must
# match once the fields that legitimately vary between runs are set aside.
class RailsAppTest < E2ETestCase
  PORT = 19240
  REFERENCE_PORT = 19241
  THREAD_ISOLATED_PORT = 19242
  CONCURRENT = 32
  APP = File.join(ROOT, "test", "fixtures", "rails_app")
  HOST = "app.test"
  SECRET = SecureRandom.hex(64)
  UPLOAD = Random.new(1).bytes(5 * 1024 * 1024)

  # Headers that vary between runs, or describe the connection rather than the answer.
  IGNORED_HEADERS = %w[date server x-request-id x-runtime connection keep-alive content-length transfer-encoding].freeze

  # Envoy fronts the application, so what it passes on carries headers a direct connection does not.
  ENVOY_ADDED_HEADERS = %w[HTTP_X_FORWARDED_PROTO].freeze

  Step = Data.define(:name, :method, :path, :headers, :body, :form, :capture, :normalize, :ignored)

  def self.step(name, method, path, headers: {}, body: nil, form: nil, capture: {}, normalize: :itself.to_proc,
                ignored: [])
    Step.new(name:, method:, path:, headers:, body:, form:, capture:, normalize:, ignored:)
  end

  JSON_HEADERS = { "content-type" => "application/json", "accept" => "application/json" }.freeze
  FORM_HEADERS = { "content-type" => "application/x-www-form-urlencoded" }.freeze
  WITHOUT_HEADERS = lambda do |body|
    JSON.generate(JSON.parse(body).except("headers"))
  rescue JSON::ParserError
    body
  end
  WITHOUT_TOKEN = ->(body) { body.gsub(/(name="csrf-token" content=")[^"]+/, '\1…') }
  # The etag digests the body as sent, before the normalizer set its varying part aside.
  ECHOED = { normalize: WITHOUT_HEADERS, ignored: %w[etag] }.freeze

  CORPUS = [
    step("list", :get, "/widgets"),
    step("list filtered by an array param", :get, "/widgets?names[]=first&names[]=second"),
    step("show", :get, "/widgets/1", capture: { etag: ->(response) { response["etag"] } }),
    step("conditional show", :get, "/widgets/1", headers: { "if-none-match" => "{{etag}}" }),
    step("missing record", :get, "/widgets/999"),
    step("missing route", :get, "/nowhere"),
    step("head", :head, "/widgets"),
    step("create", :post, "/widgets", headers: JSON_HEADERS, body: JSON.generate(widget: { name: "third" })),
    step("create invalid", :post, "/widgets", headers: JSON_HEADERS, body: JSON.generate(widget: { name: "" })),
    step("create without the root key", :post, "/widgets", headers: JSON_HEADERS, body: JSON.generate(name: "x")),
    step("malformed json", :post, "/widgets", headers: JSON_HEADERS, body: "{\"widget\":"),
    step("create from a form", :post, "/widgets", headers: FORM_HEADERS, body: "widget[name]=fourth"),
    step("update", :patch, "/widgets/1", headers: JSON_HEADERS, body: JSON.generate(widget: { name: "renamed" })),
    step("replace", :put, "/widgets/2", headers: JSON_HEADERS, body: JSON.generate(widget: { name: "replaced" })),
    step("destroy", :delete, "/widgets/2"),
    step("list after the changes", :get, "/widgets"),
    step("echo a query", :get, "/echo?x=1&y=%E4%B8%AD&list[]=a&list[]=b", **ECHOED),
    step("echo a json body", :post, "/echo", headers: JSON_HEADERS, body: JSON.generate(nested: { list: [ 1, 2 ] }),
                                            **ECHOED),
    step("echo a form body", :post, "/echo", headers: FORM_HEADERS, body: "a=1&b=%20two&c[]=x", **ECHOED),
    step("echo a large body", :put, "/echo", headers: { "content-type" => "application/octet-stream" },
                                             body: UPLOAD.byteslice(0, 2 * 1024 * 1024), **ECHOED),
    step("echo custom headers", :get, "/echo", headers: { "x-custom" => "value", "accept-language" => "zh-CN" },
                                               **ECHOED),
    step("form", :get, "/form", capture: { token: ->(response) { response.body[/name="csrf-token" content="([^"]+)"/, 1] } },
                                normalize: WITHOUT_TOKEN, ignored: %w[etag]),
    step("submit with the token", :post, "/form", headers: FORM_HEADERS, body: "authenticity_token={{token}}&name=ok"),
    step("submit without a token", :post, "/form", headers: FORM_HEADERS, body: "name=forged"),
    step("write the session", :post, "/session", headers: FORM_HEADERS, body: "name=ada"),
    step("read the session", :get, "/session"),
    step("moved", :get, "/old"),
    step("download", :get, "/download"),
    step("upload", :post, "/upload", form: [ [ "file", UPLOAD, { filename: "blob.bin", content_type: "application/octet-stream" } ],
                                             [ "note", "five megabytes" ] ]),
    step("error page", :get, "/boom"),
    step("stream", :get, "/stream")
  ].freeze

  # One pass over the corpus against one server, carrying cookies and
  # captured values from step to step.
  class Pass
    def initialize(port)
      @port = port
      @cookies = {}
      @captured = {}
    end

    def run(steps)
      steps.to_h { |step| [ step.name, answer(step) ] }
    end

    private

    def answer(step)
      response = Net::HTTP.start("127.0.0.1", @port, read_timeout: 60) { |http| http.request(request_for(step)) }
      remember(step, response)
      [ response.code, headers(response).except(*step.ignored), step.normalize.call(response.body.to_s) ]
    end

    def request_for(step)
      request = Net::HTTP.const_get(step.method.to_s.capitalize).new(fill(step.path))
      request["host"] = HOST
      step.headers.each { |name, value| request[name] = fill(value) }
      request["cookie"] = @cookies.map { |name, value| "#{name}=#{value}" }.join("; ") unless @cookies.empty?
      if step.form
        request.set_form(step.form, "multipart/form-data")
      elsif step.body
        request.body = fill(step.body)
      end
      request
    end

    def fill(text)
      text.gsub(/\{\{(\w+)\}\}/) { @captured.fetch(Regexp.last_match(1).to_sym) }
    end

    def remember(step, response)
      response.get_fields("set-cookie")&.each do |cookie|
        name, value = cookie.split(";", 2).first.split("=", 2)
        @cookies[name] = value
      end
      step.capture.each { |key, extract| @captured[key] = extract.call(response) }
    end

    def headers(response)
      response.to_hash.except(*IGNORED_HEADERS).to_h do |name, values|
        next [ name, values ] unless name == "set-cookie"

        [ name, values.map { |cookie| cookie.sub(/=[^;]*/, "=…") } ]
      end
    end
  end

  class << self
    def servers
      @servers ||= start_servers
    end

    def results
      @results ||= File.join(result_dir, "rails-app-#{run_id}").tap { |dir| FileUtils.mkdir_p(dir) }
    end

    def app_env
      { "BUNDLE_GEMFILE" => File.join(APP, "Gemfile"),
        "BUNDLE_PATH" => File.join(root, "vendor", "bundle"),
        "BUNDLE_FROZEN" => "true",
        "RAILS_ENV" => "production",
        "SECRET_KEY_BASE" => SECRET,
        "DATABASE_URL" => database_url }
    end

    # Room for every request the suites keep in flight at once.
    def database_url
      url = ENV.fetch("RUVOY_RAILS_DATABASE_URL", "postgres://bench:bench@127.0.0.1/ruvoy_rails_app")
      "#{url}#{url.include?("?") ? "&" : "?"}max_connections=64"
    end

    # Puts the seed rows back; every pass starts from here.
    def prepare_database
      log = File.join(results, "prepare.log")
      prepared = Bundler.with_unbundled_env do
        system(app_env, "bundle", "exec", "ruby", "db/prepare.rb", chdir: APP, out: log, err: %i[child out])
      end
      raise "preparing the database failed:\n#{tail(log, lines: 20)}" unless prepared
    end

    private

    def start_servers
      [ PORT, REFERENCE_PORT, THREAD_ISOLATED_PORT ].each do |port|
        raise "TCP port #{port} is already in use" unless port_free?(port)
      end
      unless Bundler.with_unbundled_env { system(app_env, "bundle", "check", chdir: APP, out: File::NULL, err: File::NULL) }
        raise "the Rails application's bundle is missing: " \
              "BUNDLE_GEMFILE=#{app_env["BUNDLE_GEMFILE"]} BUNDLE_PATH=#{app_env["BUNDLE_PATH"]} bundle install"
      end

      build_module("fiber", log: File.join(results, "build.log"))
      prepare_database
      ruvoy = start_ruvoy("rails_app", PORT, app_env)
      # The same application left on the thread isolation default.
      thread_isolated = start_ruvoy("rails_app_thread", THREAD_ISOLATED_PORT,
                                    app_env.merge("RAILS_ISOLATION_LEVEL" => "thread"))
      reference = Server.start("bundle", "exec", "puma", "-e", "production", "-b", "tcp://127.0.0.1:#{REFERENCE_PORT}",
                               "-t", "16:16", "config.ru", log: File.join(results, "puma.log"), env: app_env, chdir: APP)
      Minitest.after_run { [ ruvoy, reference, thread_isolated ].each(&:stop) }
      { ruvoy => PORT, reference => REFERENCE_PORT, thread_isolated => THREAD_ISOLATED_PORT }.each do |server, port|
        next if server.serving?("http://127.0.0.1:#{port}/widgets")

        raise "the server on #{port} did not become ready:\n#{tail(server.log, lines: 20)}"
      end
      { ruvoy: ruvoy, reference: reference, thread_isolated: thread_isolated }
    end

    def start_ruvoy(name, port, env)
      config = File.join(scratch_dir, "#{name}.yaml")
      File.write(config, listener_config(name, port))
      Envoy.start(config: config, modules: module_dir, log: File.join(results, "#{name}.log"), log_level: "info",
                  env: env)
    end

    def listener_config(name, port)
      <<~YAML
        #{bootstrap_yaml(rackup: File.join(APP, "config.ru"))}
        static_resources:
          listeners:
            - name: #{name}
              address:
                socket_address:
                  address: 127.0.0.1
                  port_value: #{port}
              filter_chains:
                - filters:
                    - name: envoy.filters.network.http_connection_manager
                      typed_config:
                        "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                        stat_prefix: #{name}
                        codec_type: AUTO
                        stream_idle_timeout: 60s
                        route_config:
                          name: rails_app_route
                          virtual_hosts:
                            - name: rails_app
                              domains: ["*"]
                        http_filters:
                          - name: envoy.extensions.filters.http.dynamic_modules
                            typed_config:
                              "@type": type.googleapis.com/envoy.extensions.filters.http.dynamic_modules.v3.DynamicModuleFilter
                              dynamic_module_config:
                                name: ruvoy_fiber
                                do_not_close: true
                              filter_name: fiber_rack
                              terminal_filter: true
                              filter_config:
                                "@type": type.googleapis.com/google.protobuf.StringValue
                                value: "{}"
      YAML
    end
  end

  def setup
    self.class.servers
  end

  def test_every_answer_matches_the_reference_server
    reference = pass(REFERENCE_PORT)
    ruvoy = pass(PORT)

    differences = CORPUS.map(&:name).reject { |name| reference[name] == ruvoy[name] }
    assert_empty differences, differences.map { |name| difference(name, reference[name], ruvoy[name]) }.join("\n")
  end

  def test_what_envoy_adds_to_the_request
    reference = echoed_headers(REFERENCE_PORT)
    ruvoy = echoed_headers(PORT)

    assert_equal ENVOY_ADDED_HEADERS, (ruvoy.keys - reference.keys).sort
    assert_empty (ruvoy.keys & reference.keys).reject { |key| reference[key] == ruvoy[key] },
                 "headers both servers pass on must arrive unchanged: reference=#{reference} ruvoy=#{ruvoy}"
  end

  def test_concurrent_requests_each_hold_their_own_connection
    concurrently(CONCURRENT) { get(PORT, "/pid?ms=300") }
    started = clock
    answers = concurrently(CONCURRENT) { get(PORT, "/pid?ms=300") }
    elapsed = clock - started

    assert_equal [ "200" ], answers.map(&:first).uniq, answers.inspect[0, 600]
    assert_equal CONCURRENT, answers.map { |_, body| JSON.parse(body)["pid"] }.uniq.size,
                 "each request in flight must hold a connection of its own"
    assert_operator elapsed, :<, 0.3 * 3, "the requests should wait together, not in turn"
  end

  def test_left_on_thread_isolation_concurrent_requests_collide
    answers = concurrently(8) { get(THREAD_ISOLATED_PORT, "/pid?ms=300") }
    pids = answers.select { |code, _| code == "200" }.map { |_, body| JSON.parse(body)["pid"] }

    refute answers.all? { |code, _| code == "200" } && pids.uniq.size == 8,
           "every fiber shares the runtime thread, so thread isolation must not give each its own connection"
  end

  def test_a_client_leaving_mid_transaction_leaves_no_row_and_frees_its_connection
    socket = TCPSocket.new("127.0.0.1", PORT)
    body = "name=abandoned"
    socket.write("POST /held_write?ms=3000 HTTP/1.1\r\nhost: #{HOST}\r\n" \
                 "content-type: application/x-www-form-urlencoded\r\ncontent-length: #{body.bytesize}\r\n\r\n#{body}")
    wait_for { pool_stat["busy"] == 1 }
    socket.close

    wait_for { pool_stat.values_at("busy", "dead") == [ 0, 0 ] }
    code, widgets = get(PORT, "/widgets")
    assert_equal "200", code
    refute_includes JSON.parse(widgets).map { it["name"] }, "abandoned", "the transaction must roll back"
  end

  private

  def get(port, path)
    response = Net::HTTP.start("127.0.0.1", port, read_timeout: 30) { |http| http.get(path, "host" => HOST) }
    [ response.code, response.body ]
  end

  def pool_stat
    JSON.parse(get(PORT, "/pool").last)
  end

  def concurrently(count, &request)
    Array.new(count) { Thread.new(&request) }.map(&:value)
  end

  def wait_for(timeout: 5)
    deadline = clock + timeout
    until yield
      flunk "timed out waiting" if clock > deadline
      sleep 0.05
    end
  end

  def pass(port)
    self.class.prepare_database
    Pass.new(port).run(CORPUS)
  end

  def echoed_headers(port)
    response = Net::HTTP.start("127.0.0.1", port) do |http|
      http.get("/echo", "host" => HOST, "x-custom" => "value", "user-agent" => "differential")
    end
    JSON.parse(response.body).fetch("headers").except("HTTP_X_REQUEST_ID")
  end

  def difference(name, reference, ruvoy)
    parts = %w[status headers body].zip(reference, ruvoy).filter_map do |part, expected, actual|
      next if expected == actual

      "  #{part}: reference=#{expected.inspect[0, 400]}\n  #{" " * part.size}  ruvoy=    #{actual.inspect[0, 400]}"
    end
    "#{name}\n#{parts.join("\n")}"
  end
end
