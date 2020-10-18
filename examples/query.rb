# frozen_string_literal: true

require "mimi/messaging/sqs_sns"

COUNT = 10
AWS_REGION            = "eu-west-1"
AWS_SQS_ENDPOINT_URL  = "http://localstack:4566"
AWS_SNS_ENDPOINT_URL  = "http://localstack:4566"
AWS_ACCESS_KEY_ID     = "foo"
AWS_SECRET_ACCESS_KEY = "bar"

logger = Logger.new(STDOUT)
logger.level = Logger::INFO
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

adapter.start

t_start = Time.now
COUNT.times do |i|
  t = Time.now
  result = adapter.query("test/hello", i: i) # rand(100))
  puts "result: #{result.to_h}, t: %.3fs" % (Time.now - t)
  sleep 1
end

t_elapsed = Time.now - t_start
puts "t_elapsed: %.3fs" % t_elapsed
adapter.stop
puts "t.avg: %.3fs" % (t_elapsed / COUNT)
