# frozen_string_literal: true

require "mimi/messaging/sqs_sns"

AWS_REGION = "eu-west-1"
AWS_SQS_ENDPOINT_URL = "http://localstack:4566"
AWS_SNS_ENDPOINT_URL = "http://localstack:4566"
AWS_ACCESS_KEY_ID = "foo"
AWS_SECRET_ACCESS_KEY = "bar"

class Processor
  def self.call_command(method_name, message, opts)
    puts "COMMAND: #{method_name}, #{message}, headers: #{opts[:headers]}"
  end

  def self.call_query(method_name, message, opts)
    # puts "QUERY: #{method_name}, #{message}, headers: #{opts[:headers]}"
    puts "QUERY: #{method_name}, headers: #{opts[:headers]}"
    {}
  end
end # class Processor


logger = Logger.new(STDOUT)
logger.level = Logger::DEBUG
Mimi::Messaging.use(logger: logger, serializer: Mimi::Messaging::JsonSerializer)
Mimi::Messaging.configure(
  mq_adapter: "sqs_sns",
  mq_aws_access_key_id:     AWS_ACCESS_KEY_ID,
  mq_aws_secret_access_key: AWS_SECRET_ACCESS_KEY,
  mq_aws_region:            AWS_REGION,
  mq_aws_sqs_endpoint:      AWS_SQS_ENDPOINT_URL,
  mq_aws_sns_endpoint:      AWS_SNS_ENDPOINT_URL
)
adapter = Mimi::Messaging.adapter
queue_name = "test"
adapter.start
puts "Registering processor on '#{queue_name}'"
adapter.start_request_processor(queue_name, Processor)


begin
  loop do
    sleep 1
  end
ensure
  puts "Stopping adapter"
  adapter.stop
end

