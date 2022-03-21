# frozen_string_literal: true

require "mimi/messaging"
require "aws-sdk-sqs"
require "aws-sdk-sns"
require "securerandom"
require "concurrent"
require_relative "timeout_queue"

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
        #
        # NOTE: AWS SQS/SNS alphabet for queue and topic names
        # is different from what mimi-messaging allows:
        # '.' is not an allowed character.
        #
        # SQS_SNS_ALPHABET_MAP structure is used to convert
        # names from mimi-messaging alphabet to SQS/SNS alphabet.
        #
        # Mimi::Messaging still accepts queue and topic names
        # containing the '.', but the adapter will convert those
        # to valid SQS/SNS names using this mapping.
        #
        SQS_SNS_ALPHABET_MAP = {
          "." => "-"
        }.freeze

        attr_reader :options, :sqs_client, :sns_client, :worker_pool

        register_adapter_name "sqs_sns"

        DEFAULT_OPTIONS = {
          mq_namespace: nil,
          mq_default_query_timeout: 15, # seconds,
          mq_reply_queue_prefix: "reply-",

          # worker pool parameters
          mq_worker_pool_min_threads: 1,
          mq_worker_pool_max_threads: 16,
          mq_worker_pool_max_backlog: 16,

          # if nil, AWS SDK will guess values from environment
          mq_aws_region: nil,
          mq_aws_access_key_id: nil,
          mq_aws_secret_access_key: nil,
          mq_aws_sqs_endpoint: nil,
          mq_aws_sns_endpoint: nil,

          mq_aws_sqs_sns_kms_master_key_id: nil,
          mq_aws_sqs_read_timeout: 20, # seconds

          mq_aws_sqs_cross_account_mapping: "",
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
        # @option options [String,nil] :mq_aws_sqs_cross_account_mapping
        #
        def initialize(options)
          @options = DEFAULT_OPTIONS.merge(options).dup

          # Split cross account mapping into Hash with `queue` => `account_id`
          @options[:cross_account_map] = Hash[
            @options[:mq_aws_sqs_cross_account_mapping].split(",").map{|e|
              e.split(":")
            }
          ]
          @reply_consumer_mutex = Mutex.new
        end

        def start
          @sqs_client = Aws::SQS::Client.new(sqs_client_config)
          @sns_client = Aws::SNS::Client.new(sns_client_config)
          start_worker_pool!
          check_availability!
        end

        def stop
          stop_all_processors
          stop_worker_pool!
          @sqs_client = nil
          @sns_client = nil
        end

        # Stops all message (command, query and event) processors.
        #
        # Stops currently registered processors and stops accepting new messages
        # for processors.
        #
        def stop_all_processors
          @consumers&.each(&:signal_stop)
          @consumers&.each(&:stop)
          @consumers = nil
          @reply_consumer&.stop
          @reply_consumer = nil
        end

        # Sends the command to the given target
        #
        # Example:
        #   Mimi::Messaging.command("users/create", name: "John Smith")
        #
        # @param target [String] "<queue>/<method>"
        # @param message [Hash,Mimi::Messaging::Message]
        # @param opts [Hash] additional adapter-specific options
        #
        # @return nil
        #
        def command(target, message, _opts = {})
          queue_name, method_name = target.split("/")
          message = Mimi::Messaging::Message.new(message, __method: method_name)
          queue_url = find_queue!(queue_name)
          deliver_message_queue(queue_url, message)
        end

        # Executes the query to the given target and returns response
        #
        # @param target [String] "<queue>/<method>"
        # @param message [Hash,Mimi::Messaging::Message]
        # @param opts [Hash] additional options, e.g. :timeout
        #
        # @return [Hash]
        # @raise [SomeError,Timeout::Error]
        #
        def query(target, message, opts = {})
          queue_name, method_name = target.split("/")
          queue_url = find_queue!(queue_name)
          request_id = SecureRandom.hex(8)
          reply_queue = reply_consumer.register_request_id(request_id)

          message = Mimi::Messaging::Message.new(
            message,
            __method: method_name,
            __reply_queue_url: reply_consumer.reply_queue_url,
            __request_id: request_id
          )
          deliver_message_queue(queue_url, message)
          timeout = opts[:timeout] || options[:mq_default_query_timeout]
          response = reply_queue.pop(true, timeout)
          deserialize(response.body)
        end

        # Broadcasts the event with the given target
        #
        # @param target [String] "<topic>#<event_type>", e.g. "customers#created"
        # @param message [Mimi::Messaging::Message]
        # @param opts [Hash] additional options
        #
        def event(target, message, _opts = {})
          topic_name, event_type = target.split("#")
          message = Mimi::Messaging::Message.new(message, __event_type: event_type)
          topic_arn = find_or_create_topic(topic_name) # TODO: or find_topic!(...) ?
          deliver_message_topic(topic_arn, message)
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
          @consumers << Consumer.new(self, queue_url, worker_pool) do |m|
            process_request_message(processor, m)
          end
        end

        def start_event_processor(topic_name, processor, opts = {})
          # NOTE: due to SQS/SNS limitations, implementing this will
          # require creating a temporary queue and subscribing it to the topic
          raise "Not implemented"
        end

        def start_event_processor_with_queue(topic_name, queue_name, processor, opts = {})
          @consumers ||= []
          opts = opts.dup
          topic_arn = find_or_create_topic(topic_name) # TODO: or find_topic!(...) ?
          queue_url = find_or_create_queue(queue_name)
          subscribe_topic_queue(topic_arn, queue_url)
          @consumers << Consumer.new(self, queue_url, worker_pool) do |m|
            process_event_message(processor, m)
          end
        end

        # Creates a new queue
        #
        # @param queue_name [String] name of the topic to be created
        # @return [String] a new queue URL
        #
        def create_queue(queue_name)
          fqn = sqs_sns_converted_full_name(queue_name)
          Mimi::Messaging.log "Creating a queue: #{fqn}"
          attrs = {}
          if options[:mq_aws_sqs_sns_kms_master_key_id]
            attrs["KmsMasterKeyId"] = options[:mq_aws_sqs_sns_kms_master_key_id]
          end
          result = sqs_client.create_queue(queue_name: fqn, attributes: attrs)
          result.queue_url
        rescue StandardError => e
          raise Mimi::Messaging::ConnectionError, "Failed to create queue '#{queue_name}': #{e}"
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

        # Deletes a queue identified by the queue URL
        #
        # @param queue_url [String]
        #
        def delete_queue(queue_url)
          Mimi::Messaging.log "Deleting a queue: #{queue_url}"
          sqs_client.delete_queue(queue_url: queue_url)
        rescue StandardError => e
          raise Mimi::Messaging::ConnectionError,
            "Failed to delete queue with url '#{queue_url}': #{e}"
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

        # Returns configuration parameters for AWS SNS client
        #
        # @return [Hash]
        #
        def sns_client_config
          params = {
            region: options[:mq_aws_region],
            endpoint: options[:mq_aws_sns_endpoint],
            access_key_id: options[:mq_aws_access_key_id],
            secret_access_key: options[:mq_aws_secret_access_key]
          }
          params.compact
        end

        # Checks SQS and SNS clients availability
        #
        # @raise [Mimi::Messaging::ConnectionError]
        #
        def check_availability!
          begin
            queue_registry("test")
          rescue StandardError => e
            raise Mimi::Messaging::ConnectionError, "SQS connection is not available: #{e}"
          end
          begin
            topic_registry("test")
          rescue StandardError => e
            raise Mimi::Messaging::ConnectionError, "SNS connection is not available: #{e}"
          end
        end

        # Delivers a message to a queue with given URL.
        #
        # @param queue_url [String]
        # @param message [Mimi::Messaging::Message]
        #
        def deliver_message_queue(queue_url, message)
          raise ArgumentError, "Non-empty queue URL is expected" unless queue_url
          unless message.is_a?(Mimi::Messaging::Message)
            raise ArgumentError, "Message is expected as argument"
          end

          Mimi::Messaging.log "Delivering message to: #{queue_url}, headers: #{message.headers}"
          sqs_client.send_message(
            queue_url: queue_url,
            message_body: serialize(message),
            message_attributes: message.headers.map do |k, v|
              [k.to_s, { data_type: "String", string_value: v.to_s }]
            end.to_h
          )
        rescue StandardError => e
          raise Mimi::Messaging::ConnectionError, "Failed to deliver message to '#{queue_url}': #{e}"
        end

        # Processes an incoming COMMAND or QUERY message
        #
        # @param processor [#call_query(),#call_command()] request processor object
        # @param sqs_message
        #
        def process_request_message(processor, sqs_message)
          message = Mimi::Messaging::Message.new(
            deserialize(sqs_message.body),
            deserialize_headers(sqs_message)
          )
          method_name = message.headers[:__method]
          reply_to = message.headers[:__reply_queue_url]
          if reply_to
            response = processor.call_query(method_name, message, {})
            response_message = Mimi::Messaging::Message.new(
              response,
              __request_id: message.headers[:__request_id]
            )
            deliver_query_response(reply_to, response_message)
          else
            processor.call_command(method_name, message, {})
          end
        end

        # Delivers a message as a response to a QUERY
        #
        # Responses are allowed to fail. There can be a number of reasons
        # why responses fail: reply queue does not exist (anymore?),
        # response message is too big. In any case the error is reported,
        # but the QUERY message is acknowledged as a successfully processed.
        #
        # @param queue_url [String]
        # @param message [Mimi::Messaging::Message]
        #
        def deliver_query_response(queue_url, message)
          deliver_message_queue(queue_url, message)
        rescue Mimi::Messaging::ConnectionError => e
          Mimi::Messaging.logger&.warn("Failed to deliver QRY response: #{e}")
          # NOTE: error is recovered
        end

        # Returns URL of a queue with a given name.
        #
        # If the queue with given name does not exist, returns nil
        #
        # If the option mq_aws_sqs_cross_account_mapping is set,
        # check queue name if it's owned by a different account
        # and if look up the URL from that other account
        #
        # @param queue_name [String]
        # @return [String,nil] queue URL
        #
        def queue_registry(queue_name)
          fqn = sqs_sns_converted_full_name(queue_name)
          @queue_registry ||= {}
          @queue_registry[fqn] ||= begin
            opts = {queue_name: fwn}
            if options[:cross_account_map][queue_name]
              opts[:queue_owner_aws_account_id] = options[:cross_account_map][queue_name]
            end
            result = sqs_client.get_queue_url(opts)
            result.queue_url
          end
        rescue Aws::SQS::Errors::NonExistentQueue
          nil
        rescue StandardError => e
          raise Mimi::Messaging::ConnectionError, "Failed to get queue url '#{queue_name}': #{e}"
        end

        # Converts a topic or queue name to a fully qualified (with namespace)
        # and in a valid SQS/SNS alphabet.
        #
        # @param name [String] a mimi-messaging valid name
        # @return [String] an SQS/SNS valid name
        #
        def sqs_sns_converted_full_name(name)
          name = "#{options[:mq_namespace]}#{name}"
          SQS_SNS_ALPHABET_MAP.each do |from, to|
            name = name.gsub(from, to)
          end
          name
        end

        # Finds a queue URL for a queue with a given name,
        # or raises an error if the queue is not found.
        #
        # @param queue_name [String]
        # @return [String] a queue URL
        #
        def find_queue!(queue_name)
          queue_registry(queue_name) || (
            raise Mimi::Messaging::ConnectionError,
              "Failed to find a queue with given name: '#{queue_name}'"
          )
        end

        # Returns the configured reply listener for this process
        #
        # @return [ReplyConsumer]
        #
        def reply_consumer
          @reply_consumer_mutex.synchronize do
            @reply_consumer ||= begin
              reply_queue_name = options[:mq_reply_queue_prefix] + SecureRandom.hex(8)
              Mimi::Messaging::SQS_SNS::ReplyConsumer.new(self, reply_queue_name)
            end
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

        # Lists all SNS topics by their ARNs.
        #
        # NOTE: iterates over all topics at SNS every time
        #
        # @return [Array<String>] array of topic ARNs
        #
        def sns_list_topics
          result = []
          next_token = nil
          loop do
            response = sns_client.list_topics(next_token: next_token)
            result += response.topics.map(&:topic_arn)
            next_token = response.next_token
            break unless next_token
          end
          result
        rescue StandardError => e
          raise Mimi::Messaging::ConnectionError, "Failed to list topics: #{e}"
        end

        # Returns ARN of a topic with a given name.
        #
        # If the topic with given name does not exist, returns nil
        #
        # @param topic_name [String]
        # @return [String,nil] topic ARN or nil, if not found
        #
        def topic_registry(topic_name)
          fqn = sqs_sns_converted_full_name(topic_name)
          @topic_registry ||= {}
          @topic_registry[fqn] ||= begin
            sns_list_topics.find { |topic_arn| topic_arn.split(":").last == fqn }
          end
        end

        # Finds a topic ARN for a topic with a given name,
        # or raises an error if the topic is not found.
        #
        # @param topic_name [String]
        # @return [String] a topic ARN
        #
        def find_topic!(topic_name)
          topic_registry(topic_name) || (
            raise Mimi::Messaging::ConnectionError,
              "Failed to find a topic with given name: '#{topic_name}'"
          )
        end

        # Finds a topic ARN for a topic with given name.
        #
        # If an existing topic with this name is not found,
        # the method will try to create a new one.
        #
        # @param topic_name [String]
        # @return [String] a topic ARN
        #
        def find_or_create_topic(topic_name)
          topic_registry(topic_name) || create_topic(topic_name)
        end

        # Creates a new topic
        #
        # @param topic_name [String] name of the topic to be created
        # @return [String] a new topic ARN
        #
        def create_topic(topic_name)
          fqn = sqs_sns_converted_full_name(topic_name)
          Mimi::Messaging.log "Creating a topic: #{fqn}"
          attrs = {}
          if options[:mq_aws_sqs_sns_kms_master_key_id]
            attrs["KmsMasterKeyId"] = options[:mq_aws_sqs_sns_kms_master_key_id]
          end
          result = sns_client.create_topic(name: fqn, attributes: attrs)
          result.topic_arn
        rescue StandardError => e
          raise Mimi::Messaging::ConnectionError, "Failed to create topic '#{topic_name}': #{e}"
        end

        # Subscribes an existing queue to an existing topic
        #
        # @param topic_arn [String]
        # @param queue_url [String]
        #
        def subscribe_topic_queue(topic_arn, queue_url)
          result = sqs_client.get_queue_attributes(
            queue_url: queue_url, attribute_names: ["QueueArn"]
          )
          queue_arn = result.attributes["QueueArn"]
          Mimi::Messaging.log "Subscribing queue to a topic: '#{topic_arn}'->'#{queue_url}'"
          _result = sns_client.subscribe(
            topic_arn: topic_arn,
            protocol: "sqs",
            endpoint: queue_arn,
            attributes: { "RawMessageDelivery" => "true" }
          )
          true
        rescue StandardError => e
          raise Mimi::Messaging::ConnectionError,
            "Failed to subscribe queue to topic '#{topic_arn}'->'#{queue_url}': #{e}"
        end

        # Delivers a message to a topic with given ARN.
        #
        # @param topic_arn [String]
        # @param message [Mimi::Messaging::Message]
        #
        def deliver_message_topic(topic_arn, message)
          raise ArgumentError, "Non-empty topic ARN is expected" unless topic_arn
          unless message.is_a?(Mimi::Messaging::Message)
            raise ArgumentError, "Message is expected as argument"
          end

          Mimi::Messaging.log "Delivering message to: #{topic_arn}"
          sns_client.publish(
            topic_arn: topic_arn,
            message: serialize(message),
            message_attributes: message.headers.map do |k, v|
              [k.to_s, { data_type: "String", string_value: v.to_s }]
            end.to_h
          )
        rescue StandardError => e
          raise Mimi::Messaging::ConnectionError, "Failed to deliver message to '#{topic_arn}': #{e}"
        end

        # Processes an incoming EVENT message
        #
        # @param processor [#call_event()] event processor object
        # @param sqs_message []
        #
        def process_event_message(processor, sqs_message)
          message = Mimi::Messaging::Message.new(
            deserialize(sqs_message.body),
            deserialize_headers(sqs_message)
          )
          event_type = message.headers[:__event_type]
          processor.call_event(event_type, message, {})
        end

        # Starts the worker pool using current configuration
        #
        # @return [Concurrent::ThreadPoolExecutor]
        #
        def start_worker_pool!
          Mimi::Messaging.log "Starting worker pool, " \
            "min_threads:#{options[:mq_worker_pool_min_threads]}, " \
            "max_threads:#{options[:mq_worker_pool_max_threads]}, " \
            "max_backlog:#{options[:mq_worker_pool_max_backlog]}"

          @worker_pool = Concurrent::ThreadPoolExecutor.new(
            min_threads: options[:mq_worker_pool_min_threads],
            max_threads: options[:mq_worker_pool_max_threads],
            max_queue: options[:mq_worker_pool_max_backlog],
            fallback_policy: :abort
          )
        end

        # Gracefully stops the worker pool, allowing all threads to finish their jobs
        #
        def stop_worker_pool!
          Mimi::Messaging.log "Stopping worker pool"
          @worker_pool.shutdown
          @worker_pool.wait_for_termination
        end
      end # class Adapter
    end # module SQS_SNS
  end # module Messaging
end # module Mimi
