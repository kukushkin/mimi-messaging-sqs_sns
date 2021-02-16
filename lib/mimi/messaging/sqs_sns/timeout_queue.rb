# frozen_string_literal: true

require "timeout"

module Mimi
  module Messaging
    module SQS_SNS
      # TimeoutQueue solves the problem the native Ruby Queue class has with waiting for elements.
      #
      # See the excellent blog post discussing the issue:
      # https://medium.com/workday-engineering/ruby-concurrency-building-a-timeout-queue-5d7c588ca80d
      #
      # TLDR -- using Ruby standard Timeout.timeout() around Queue#pop() is unsafe
      #
      #
      class TimeoutQueue
        def initialize
          @elems = []
          @mutex = Mutex.new
          @cond_var = ConditionVariable.new
        end

        # Pushes an element into the queue
        #
        # @param elem [Object]
        #
        def <<(elem)
          @mutex.synchronize do
            @elems << elem
            @cond_var.signal
          end
        end
        alias push <<

        # Pops an element from the queue in either non-blocking
        # or a blocking (with an optional timeout) way.
        #
        # @param blocking [true,false] wait for a new element (true) or return immediately
        # @param timeout [nil,Integer] if in blocking mode,
        #                              wait at most given number of seconds or forever (nil)
        # @raise [Timeout::Error] if a timeout in blocking mode was reached
        #
        def pop(blocking = true, timeout = nil)
          @mutex.synchronize do
            if blocking
              if timeout.nil?
                while @elems.empty?
                  @cond_var.wait(@mutex)
                end
              else
                timeout_time = Time.now.to_f + timeout
                while @elems.empty? && (remaining_time = timeout_time - Time.now.to_f) > 0
                  @cond_var.wait(@mutex, remaining_time)
                end
              end
            end
            raise Timeout::Error, "queue timeout expired" if @elems.empty?

            @elems.shift
          end
        end
      end # class TimeoutQueue
    end # module SQS_SNS
  end # module Messaging
end # module Mimi
