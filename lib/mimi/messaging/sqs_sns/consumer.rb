# frozen_string_literal: true

module Mimi
  module Messaging
    module SQS_SNS
      #
      # Message consumer for SQS queues
      #
      class Consumer
        # (seconds) determines how soon the NACK-ed message becomes visible to other consumers
        NACK_VISIBILITY_TIMEOUT = 1

        # Creates a new Consumer to read and process messages from the given queue.
        #
        # @param adapter [Mimi::Messaging::SQS_SNS::Adapter]
        # @param queue_url [String]
        # @param worker_pool [Concurrent::ThreadPoolExecutor, nil] an optional worker pool to be used
        #
        def initialize(adapter, queue_url, worker_pool = nil, &block)
          @stop_requested = false
          @worker_pool = worker_pool
          Mimi::Messaging.log "Starting consumer for: #{queue_url}, " \
            " worker_pool: #{worker_pool ? "yes" : "no"}"
          @consumer_thread = Thread.new do
            while not @stop_requested
              read_and_process_message(adapter, queue_url, worker_pool, block)
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
        # @param worker_pool [Concurrent::ThreadPoolExecutor,nil]
        # @param block [Proc] a block to be invoked when a message is received
        #
        def read_and_process_message(adapter, queue_url, worker_pool, block)
          message = read_message(adapter, queue_url)
          return unless message

          Mimi::Messaging.log "Read message from: #{queue_url}"
          if worker_pool
            process_message_worker_pool(adapter, queue_url, worker_pool, message, block)
          else
            process_message(adapter, queue_url, message, block)
          end
        rescue StandardError => e
          Mimi::Messaging.logger&.error(
            "#{self.class}: failed to read or process message from: #{queue_url}," \
            " worker_pool: #{worker_pool ? "yes" : "no"}" \
            " error: (#{e.class}) #{e}"
          )
        end

        # Returns a message read from SQS queue
        #
        # @param adapter [Mimi::Messaging::SQS_SNS::Adapter]
        # @param queue_url [String]
        # @return [Hash] with stringified keys
        #
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

        # Processes a received message using the worker pool
        #
        # @param adapter [Mimi::Messaging::SQS_SNS::Adapter]
        # @param queue_url [String]
        # @param worker_pool [Concurrent::ThreadPoolExecutor,nil]
        # @param block [Proc] a block to be invoked when a message is received
        #
        def process_message_worker_pool(adapter, queue_url, worker_pool, message, block)
          begin
            worker_pool.post do
              process_message(adapter, queue_url, message, block)
            end
          rescue Concurrent::RejectedExecutionError
            # the backlog is overflown, put the message back
            Mimi::Messaging.log "Worker pool backlog is full, nack-ing the message " \
              "(workers:#{worker_pool.length}, backlog:#{worker_pool.queue_length})"
            nack_message(adapter, queue_url, message)
          end
        end

        def process_message(adapter, queue_url, message, block)
          block.call(message)
          ack_message(adapter, queue_url, message)
        rescue Mimi::Messaging::NACK
          Mimi::Messaging.log "NACK-ing message from: #{queue_url}"
          nack_message(adapter, queue_url, message)
        rescue StandardError => e
          Mimi::Messaging.logger&.error(
            "#{self.class}: failed to process message from: #{queue_url}," \
            " error: (#{e.class}) #{e}"
          )
          # NOTE: error is recovered and the message is neither ACKed or NACKed
        end

        # ACK-ing the message indicates successfull processing of it
        # and removes the message from the queue
        #
        def ack_message(adapter, queue_url, msg)
          adapter.sqs_client.delete_message(
            queue_url: queue_url, receipt_handle: msg.receipt_handle
          )
        end

        # NACK-ing the message indicates a failure to process the message.
        # The message becomes immediately available to other consumers.
        #
        def nack_message(adapter, queue_url, msg)
          adapter.sqs_client.change_message_visibility(
            queue_url: queue_url,
            receipt_handle: msg.receipt_handle,
            visibility_timeout: NACK_VISIBILITY_TIMEOUT
          )
        rescue StandardError => e
          Mimi::Messaging.logger&.error(
            "#{self.class}: failed to NACK message from: #{queue_url}," \
            " error: (#{e.class}) #{e}"
          )
          # NOTE: error is recovered and the message is neither ACKed or NACKed
        end
      end # class Consumer
    end # module SQS_SNS
  end # module Messaging
end # module Mimi
