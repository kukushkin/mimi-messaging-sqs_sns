# frozen_string_literal: true

module Mimi
  module Messaging
    module SQS_SNS
      #
      # Message consumer for SQS queues
      #
      class Consumer
        def initialize(adapter, queue_url, &block)
          @stop_requested = false
          Mimi::Messaging.log "Starting consumer for: #{queue_url}"
          @consumer_thread = Thread.new do
            while not @stop_requested
              read_and_process_message(adapter, queue_url, block)
            end
            Mimi::Messaging.log "Stopping consumer for: #{queue_url}"
          end
        end

        # Requests the Consumer to stop, without actually waiting for it
        #
        def signal_stop
          @stop_requested = true
        end

        # Requests the Consumer to stop AND waits until it does
        #
        def stop
          @stop_requested = true
          @consumer_thread.join
        end

        private

        # A method invoked in a loop to read/wait for a message
        # from the associated queue and process it
        #
        # @param adapter [Mimi::Messaging::SQS_SNS::Adapter]
        # @param queue_url [String]
        # @param block [Proc] a block to be invoked when a message is received
        #
        def read_and_process_message(adapter, queue_url, block)
          message = read_message(adapter, queue_url)
          return unless message

          Mimi::Messaging.log "Read message from: #{queue_url}"
          block.call(message)
          ack_message(adapter, queue_url, message)
        rescue StandardError => e
          Mimi::Messaging.logger&.error(
            "#{self.class}: failed to read and process message from: #{queue_url}," \
            " error: (#{e.class}) #{e}"
          )
        end

        def read_message(adapter, queue_url)
          result = adapter.sqs_client.receive_message(
            queue_url: queue_url,
            max_number_of_messages: 1,
            wait_time_seconds: adapter.options[:mq_aws_sqs_read_timeout],
            message_attribute_names: ["All"]
          )
          return nil if result.messages.count == 0
          return result.messages.first if result.messages.count == 1

          raise Mimi::Messaging::ConnectionError, "Unexpected number of messages read"
        end

        def ack_message(adapter, queue_url, msg)
          adapter.sqs_client.delete_message(
            queue_url: queue_url, receipt_handle: msg.receipt_handle
          )
        end
      end # class Consumer
    end # module SQS_SNS
  end # module Messaging
end # module Mimi
