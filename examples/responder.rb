# frozen_string_literal: true

require "mimi/messaging/sqs_sns"

AWS_REGION            = "eu-west-1"
AWS_SQS_ENDPOINT_URL  = "http://sqs-sns:4566"
AWS_SNS_ENDPOINT_URL  = "http://sqs-sns:4566"
AWS_ACCESS_KEY_ID     = "foo"
AWS_SECRET_ACCESS_KEY = "bar"
AWS_SQS_SNS_KMS_MASTER_KEY_ID = "blah"

class Processor
  def self.call_command(method_name, message, opts)
    puts "COMMAND: #{method_name}, #{message}, headers: #{opts[:headers]}"
  end

  def self.call_query(method_name, message, opts)
    # puts "QUERY: #{method_name}, #{message}, headers: #{opts[:headers]}"
    puts "QUERY: #{method_name}, headers: #{opts[:headers]}"
    sleep 1 # imitate work
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
  mq_aws_sns_endpoint:      AWS_SNS_ENDPOINT_URL,
  mq_aws_sqs_sns_kms_master_key_id: AWS_SQS_SNS_KMS_MASTER_KEY_ID,
  mq_worker_pool_min_threads: 1,
  mq_worker_pool_max_threads: 2,
  mq_worker_pool_max_backlog: 4,
  mq_log_at_level: :info
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
