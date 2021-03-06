# frozen_string_literal: true

module Mimi
  module Messaging
    module SQS_SNS
      #
      # ReplyConsumer listens on a particular SQS queue for replies
      # and passes them to registered Queues (see Ruby ::Queue class).
      #
      class ReplyConsumer
        attr_reader :reply_queue_name, :reply_queue_url

        def initialize(adapter, reply_queue_name)
          @mutex = Mutex.new
          @queues = {}
          @adapter = adapter
          @reply_queue_name = reply_queue_name
          @consumer = TemporaryQueueConsumer.new(adapter, reply_queue_name) do |message|
            dispatch_message(message)
          end
          @reply_queue_url = @consumer.queue_url
        end

        def stop
          @consumer.stop
        rescue StandardError => e
          raise Mimi::Messaging::Error, "Failed to stop reply consumer: #{e}"
        end

        # Register a new request_id to listen for.
        #
        # Whenever the message with given request_id will be received,
        # it will be dispatched to a returned Queue.
        #
        # @param request_id [String]
        # @return [Queue] a new Queue object registered for this request_id
        #
        def register_request_id(request_id)
          queue = TimeoutQueue.new
          @mutex.synchronize do
            queue = @queues[request_id] ||= queue
          end
          queue
        end

        private

        # Deserializes headers from the message
        #
        # @param message
        # @return [Hash<Symbol,String>] symbolized keys, string values
        #
        def deserialize_headers(message)
          message.message_attributes.to_h.map { |k, v| [k.to_sym, v.string_value] }.to_h
        end

        # Dispatch message received on a reply queue
        #
        # @param message [] an AWS SQS message
        #
        def dispatch_message(message)
          queue = nil
          @mutex.synchronize do
            headers = deserialize_headers(message)
            request_id = headers[:__request_id]
            Mimi::Messaging.log "dispatching response, headers:#{headers}"
            queue = @queues.delete(request_id)
          end
          queue&.push(message)
        rescue StandardError => e
          Mimi::Messaging.log "reply listener failed to process reply: #{e}"
          # TODO: propagate exception to main thread?
        end
      end # class ReplyConsumer
    end # module SQS_SNS
  end # module Messaging
end # module Mimi
