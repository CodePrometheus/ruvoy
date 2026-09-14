# frozen_string_literal: true

# ActionController::Live runs the action on a thread of its own and streams
# what it writes.
class StreamsController < ActionController::Base
  include ActionController::Live

  def show
    response.headers["content-type"] = "text/plain"
    3.times do |index|
      response.stream.write("chunk #{index}\n")
      sleep 0.05
    end
  ensure
    response.stream.close
  end
end
