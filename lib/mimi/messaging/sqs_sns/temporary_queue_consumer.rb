# frozen_string_literal: true

module Mimi
  module Messaging
    module SQS_SNS
      #
      # Temporary queue consumer creates a temporary queue
      # and attaches to it. The queue will be deleted
      # on consumer shutdown.
      #
      class TemporaryQueueConsumer
        attr_reader :queue_url

        def initialize(adapter, queue_name, &block)
          @adapter = adapter
          @queue_url = adapter.find_or_create_queue(queue_name)
          @consumer = Consumer.new(adapter, @queue_url, &block)
        end

        def stop
          @consumer.stop
          @adapter.delete_queue(queue_url)
        rescue StandardError => e
          raise Mimi::Messaging::Error, "Failed to stop temporary queue consumer: #{e}"
        end
      end # class TemporaryQueueConsumer
    end # module SQS_SNS
  end # module Messaging
end # module Mimi
