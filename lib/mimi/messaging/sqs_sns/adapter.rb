# frozen_string_literal: true

require "mimi/messaging"
require "aws-sdk-sqs"
require "timeout"
require "securerandom"

module Mimi
  module Messaging
    module SQS_SNS
      #
      # AWS SQS/SNS adapter class
      #
      # An adapter implementation must implement the following methods:
      # * #start()
      # * #stop()
      # * #command(target, message, opts)
      # * #query(target, message, opts)
      # * #event(target, message, opts)
      # * #start_request_processor(queue_name, processor, opts)
      # * #start_event_processor(topic_name, processor, opts)
      # * #start_event_processor_with_queue(topic_name, queue_name, processor, opts)
      # * #stop_all_processors
      #
      class Adapter < Mimi::Messaging::Adapters::Base
        attr_reader :options, :sqs_client

        register_adapter_name "sqs_sns"

        DEFAULT_OPTIONS = {
          mq_namespace: nil,
          mq_default_query_timeout: 15, # seconds,
          mq_reply_queue_prefix: "reply.",

          # if nil, AWS SDK will guess values from environment
          mq_aws_region: nil,
          mq_aws_access_key_id: nil,
          mq_aws_secret_access_key: nil,
          mq_aws_sqs_endpoint: nil,

          mq_aws_sqs_read_timeout: 10, # seconds
        }.freeze

        # Initializes SQS/SNS adapter
        #
        # @param options [Hash]
        # @option options [String] :mq_adapter
        # @option options [String,nil] :mq_aws_region
        # @option options [String,nil] :mq_aws_access_key_id
        # @option options [String,nil] :mq_aws_secret_access_key
        # @option options [String,nil] :mq_aws_sqs_endpoint
        # @option options [String,nil] :mq_namespace
        # @option options [String,nil] :mq_reply_queue_prefix
        # @option options [Integer,nil] :mq_default_query_timeout
        #
        def initialize(options)
          @options = DEFAULT_OPTIONS.merge(options).dup
        end

        def start
          @sqs_client = Aws::SQS::Client.new(sqs_client_config)
        end

        def stop
          @consumers&.each(&:stop)
          @consumers = nil
          @reply_listener&.stop
          @reply_listener = nil
          @sqs_client = nil
        end

        # Sends the command to the given target
        #
        # Example:
        #   Mimi::Messaging.command("users/create", name: "John Smith")
        #
        # @param target [String] "<queue>/<method>"
        # @param message [Hash]
        # @param opts [Hash] additional adapter-specific options
        #
        # @return nil
        #
        def command(target, message, _opts = {})
          queue_name, method_name = target.split("/")
          message_payload = serialize(message)
          queue_url = find_queue(queue_name)
          deliver_message(message_payload, queue_url, __method: method_name)
        end

        # Executes the query to the given target and returns response
        #
        # @param target [String] "<queue>/<method>"
        # @param message [Hash]
        # @param opts [Hash] additional options, e.g. :timeout
        #
        # @return [Hash]
        # @raise [SomeError,TimeoutError]
        #
        def query(target, message, opts = {})
          queue_name, method_name = target.split("/")
          message_payload = serialize(message)
          queue_url = find_queue(queue_name)
          request_id = SecureRandom.hex(8)
          reply_queue = reply_listener.register_request_id(request_id)

          deliver_message(
            message_payload,
            queue_url,
            __method: method_name,
            __reply_queue_url: reply_listener.reply_queue_url,
            __request_id: request_id
          )
          timeout = opts[:timeout] || options[:mq_default_query_timeout]
          response = nil
          Timeout::timeout(timeout) do
            response = reply_queue.pop
          end
          deserialize(response.body)
        end

        def event(target, message, _opts = {})
          raise "Not implemented"
        end

        # Starts a request (command/query) processor.
        #
        # Processor must respond to #call_command() AND #call_query()
        # which accepts 3 arguments: (method, message, opts).
        #
        # If the processor raises an error, the message will be NACK-ed and accepted again
        # at a later time.
        #
        # @param queue_name [String] "<queue>"
        # @param processor [#call_command(),#call_query()]
        # @param opts [Hash] additional adapter-specific options
        #
        def start_request_processor(queue_name, processor, opts = {})
          super
          @consumers ||= []
          opts = opts.dup
          queue_url = find_or_create_queue(queue_name)
          @consumers << Consumer.new(self, queue_url) do |m|
            message = deserialize(m.body)
            headers = deserialize_headers(m)
            method_name = headers[:__method]
            reply_to = headers[:__reply_queue_url]
            if reply_to
              response = processor.call_query(method_name, message, headers: headers)
              deliver_message(serialize(response), reply_to, __request_id: headers[:__request_id])
            else
              processor.call_command(method_name, message, headers: headers)
            end
          end
        end

        def start_event_processor(topic_name, processor, opts = {})
          raise "Not implemented"
        end

        def start_event_processor_with_queue(topic_name, queue_name, processor, opts = {})
          raise "Not implemented"
        end

        # Stops all message (command, query and event) processors.
        #
        # Stops currently registered processors and stops accepting new messages
        # for processors.
        #
        def stop_all_processors
          @consumers.each(&:stop)
          @consumers = []
          @reply_listener = nil
        end

        # Creates a new queue
        #
        # @param queue_name [String] name of the topic to be created
        # @return [String] a new queue URL
        #
        def create_queue(queue_name)
          fqn = full_queue_name(queue_name)
          Mimi::Messaging.log "Creating a queue: #{fqn}"
          result = sqs_client.create_queue(queue_name: fqn)
          result.queue_url
        rescue StandardError => e
          raise Mimi::Messaging::ConnectionError, "Failed to create queue '#{queue_name}': #{e}"
        end

        private

        # Returns configuration parameters for AWS SQS client
        #
        # @return [Hash]
        #
        def sqs_client_config
          params = {
            region: options[:mq_aws_region],
            endpoint: options[:mq_aws_sqs_endpoint],
            access_key_id: options[:mq_aws_access_key_id],
            secret_access_key: options[:mq_aws_secret_access_key]
          }
          params.compact
        end

        # Delivers a message to a queue with given URL.
        #
        # @param message [String]
        # @param queue_url [String]
        # @param headers [Hash<Symbol,String>]
        #
        def deliver_message(message, queue_url, headers = {})
          raise ArgumentError, "Non-empty queue URL is expected" unless queue_url
          Mimi::Messaging.log "Delivering message to: #{queue_url}"
          sqs_client.send_message(
            queue_url: queue_url,
            message_body: message,
            message_attributes: headers.map do |k, v|
              [k.to_s, { data_type: "String", string_value: v.to_s }]
            end.to_h
          )
        rescue StandardError => e
          raise Mimi::Messaging::ConnectionError, "Failed to deliver message to '#{queue_url}': #{e}"
        end

        # Returns URL of a queue with a given name.
        #
        # If the queue with given name does not exist, returns nil
        #
        # @param queue_name [String]
        # @return [String,nil] queue URL
        #
        def queue_registry(queue_name)
          fqn = full_queue_name(queue_name)
          @queue_registry ||= {}
          @queue_registry[fqn] ||= begin
            result = sqs_client.get_queue_url(queue_name: fqn)
            result.queue_url
          end
        rescue Aws::SQS::Errors::NonExistentQueue
          nil
        end

        # Converts a queue name to a fully qualified queue name
        #
        # @param queue_name [String]
        # @return [String]
        #
        def full_queue_name(queue_name)
          "#{options[:mq_namespace]}#{queue_name}"
        end

        # Finds a queue URL for a queue with a given name,
        # or raises an error if the queue is not found.
        #
        # @param queue_name [String]
        # @return [String] a queue URL
        #
        def find_queue(queue_name)
          queue_registry(queue_name) || (
            raise Mimi::Messaging::ConnectionError,
              "Failed to find a queue with given name: '#{queue_name}'"
          )
        end

        # Finds a queue URL for a queue with given name.
        #
        # If an existing queue with this name is not found,
        # the method will try to create a new one.
        #
        # @param queue_name [String]
        # @return [String] a queue URL
        #
        def find_or_create_queue(queue_name)
          queue_registry(queue_name) || create_queue(queue_name)
        end

        # Returns the configured reply listener for this process
        #
        # @return [ReplyListener]
        #
        def reply_listener
          @reply_listener ||= begin
            reply_queue_name = options[:mq_reply_queue_prefix] + SecureRandom.hex(8)
            Mimi::Messaging::SQS_SNS::ReplyListener.new(self, reply_queue_name)
          end
        end

        # Deserializes headers from the message
        #
        # @param message
        # @return [Hash<Symbol,String>] symbolized keys, string values
        #
        def deserialize_headers(message)
          message.message_attributes.to_h.map { |k, v| [k.to_sym, v.string_value] }.to_h
        end
      end # class Adapter
    end # module SQS_SNS
  end # module Messaging
end # module Mimi
