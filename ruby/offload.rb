# Runs blocking calls on their own Ruby threads, so the runtime thread keeps
# serving other requests while they wait.
#
# A C extension that releases the GVL while it blocks stops only the thread it
# runs on. Moving the whole call to another thread leaves the reactor free, and
# keeps whatever the call reads from its own thread, errno included, consistent.
# An interrupted caller still waits for its call to finish before it unwinds:
# the call may be using a connection the caller's cleanup is about to hand back.
module Ruvoy
  class Offload
    def initialize(limit: nil)
      unless limit.nil? || (limit.is_a?(Integer) && limit.positive?)
        raise ArgumentError, "limit must be a positive Integer, got #{limit.inspect}"
      end

      @semaphore = limit && Async::Semaphore.new(limit)
    end

    # Runs the block on another thread and returns its value, or raises what it raised.
    def call(&work)
      raise ArgumentError, "a block is required" unless work

      @semaphore ? @semaphore.acquire { run(work) } : run(work)
    end

    private

    def run(work)
      thread = Thread.new do
        [ true, work.call ]
      rescue Exception => error
        [ false, error ]
      end
      thread.name = "ruvoy-offload"
      succeeded, result = wait_for(thread)
      succeeded ? result : raise(result)
    end

    # Interrupts that arrive before the call has finished wait for it too.
    def wait_for(thread)
      thread.value
    rescue Exception => interruption
      begin
        thread.join
      rescue Exception
        retry
      end
      raise interruption
    end
  end

  UNLIMITED_OFFLOAD = Offload.new
  private_constant :UNLIMITED_OFFLOAD

  # Runs the block on another Ruby thread; see Ruvoy::Offload.
  def self.offload(&work) = UNLIMITED_OFFLOAD.call(&work)
end
