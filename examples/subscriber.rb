# frozen_string_literal: true

require "mimi/messaging/sqs_sns"

AWS_REGION = "eu-west-1"
AWS_SQS_ENDPOINT_URL  = "http://localstack:4566"
AWS_SNS_ENDPOINT_URL  = "http://localstack:4566"
AWS_ACCESS_KEY_ID = "foo"
AWS_SECRET_ACCESS_KEY = "bar"

class Processor
  def self.call_command(method_name, message, opts)
    puts "COMMAND: #{method_name}, #{message}, headers: #{message.headers}"
  end

  def self.call_query(method_name, message, opts)
    puts "QUERY: #{method_name}, #{message}, headers: #{message.headers}"
    {}
  end

  def self.call_event(event_type, message, opts)
    puts "EVENT: #{event_type}, #{message}, headers: #{message.headers}"
    sleep 1 # imitate work
  end
end # class Processor


logger = Logger.new(STDOUT)
logger.level = Logger::INFO
Mimi::Messaging.use(logger: logger, serializer: Mimi::Messaging::JsonSerializer)
Mimi::Messaging.configure(
  mq_adapter: "sqs_sns",
  mq_aws_access_key_id:     AWS_ACCESS_KEY_ID,
  mq_aws_secret_access_key: AWS_SECRET_ACCESS_KEY,
  mq_aws_region:            AWS_REGION,
  mq_aws_sqs_endpoint:      AWS_SQS_ENDPOINT_URL,
  mq_aws_sns_endpoint:      AWS_SNS_ENDPOINT_URL,
  mq_worker_pool_min_threads: 1,
  mq_worker_pool_max_threads: 2,
  mq_worker_pool_max_backlog: 4,
  mq_log_at_level: :info
)
adapter = Mimi::Messaging.adapter

topic_name = "hello"
queue_name = "listener.hello"
adapter.start
puts "Registering event processor on '#{topic_name}'->'#{queue_name}'"
adapter.start_event_processor_with_queue(topic_name, queue_name, Processor)

begin
  loop do
    sleep 1
  end
ensure
  puts "Stopping adapter"
  adapter.stop
end
