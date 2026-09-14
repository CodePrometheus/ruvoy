# frozen_string_literal: true

require "digest"

# The parts of Rails that lean on the server: request parsing, cookies and
# sessions, forgery protection, redirects, binary bodies, error pages and
# database connections held across a wait.
class ProbesController < ActionController::Base
  skip_forgery_protection only: %i[echo upload write_session held_write]

  # What the application sees of the request.
  def echo
    render json: {
      method: request.method,
      path: request.path,
      query: request.query_parameters,
      body: request.request_parameters,
      raw_body: { bytes: request.raw_post.bytesize, sha256: Digest::SHA256.hexdigest(request.raw_post) },
      content_type: request.content_type,
      host: request.host,
      port: request.port,
      scheme: request.scheme,
      remote_ip: request.remote_ip,
      server_protocol: request.env["SERVER_PROTOCOL"],
      headers: request.headers.env.select { |key, _| key.start_with?("HTTP_") }.sort.to_h
    }
  end

  def form
    render html: helpers.csrf_meta_tags
  end

  def submit
    render plain: "accepted #{params.expect(:name)}"
  end

  def write_session
    session[:name] = params.expect(:name)
    cookies[:plain] = "visible"
    cookies.signed[:signed] = "signed value"
    head :no_content
  end

  def read_session
    render json: { name: session[:name], plain: cookies[:plain], signed: cookies.signed[:signed] }
  end

  def moved
    redirect_to widgets_path, status: :moved_permanently
  end

  def download
    send_data (0..255).map(&:chr).join.b * 64, filename: "bytes.bin", type: "application/octet-stream"
  end

  def upload
    file = params.expect(:file)
    render json: {
      filename: file.original_filename,
      content_type: file.content_type,
      bytes: file.size,
      sha256: Digest::SHA256.file(file.path).hexdigest,
      note: params[:note]
    }
  end

  def boom
    raise "boom"
  end

  # Holds a connection inside a transaction for a while and reports which one it was.
  def pid
    Widget.transaction do
      Widget.with_connection do |connection|
        pid = connection.select_value("SELECT pg_backend_pid()")
        connection.select_value(Widget.sanitize_sql_array([ "SELECT pg_sleep(?)", pause ]))
        render json: { pid: pid }
      end
    end
  end

  # Writes a row and then waits inside the same transaction, so a client that
  # leaves meanwhile should leave nothing behind.
  def held_write
    Widget.transaction do
      Widget.create!(name: params.expect(:name))
      Widget.with_connection { it.select_value(Widget.sanitize_sql_array([ "SELECT pg_sleep(?)", pause ])) }
    end
    head :no_content
  end

  def pool
    render json: Widget.connection_pool.stat
  end

  private

  def pause
    Float(params.fetch(:ms, "200")) / 1000
  end
end
