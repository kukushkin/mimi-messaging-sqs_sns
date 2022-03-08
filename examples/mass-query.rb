# frozen_string_literal: true

require "mimi/messaging/sqs_sns"

COUNT = 10
THREADS = 10
QUERY_TIMEOUT = 60
AWS_REGION            = "eu-west-1"
AWS_SQS_ENDPOINT_URL  = "http://sqs-sns:4566"
AWS_SNS_ENDPOINT_URL  = "http://sqs-sns:4566"
AWS_ACCESS_KEY_ID     = "foo"
AWS_SECRET_ACCESS_KEY = "bar"
AWS_SQS_SNS_KMS_MASTER_KEY_ID = "blah"

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
  mq_aws_sqs_sns_kms_master_key_id: AWS_SQS_SNS_KMS_MASTER_KEY_ID,
  mq_default_query_timeout: QUERY_TIMEOUT
)
adapter = Mimi::Messaging.adapter

adapter.start

t_start = Time.now
t_queries = []
threads = []
THREADS.times do |ti|
  threads << Thread.new do
    COUNT.times do |i|
      t = Time.now
      result = adapter.query("test/hello", i: i) # rand(100))
      t = Time.now - t
      t_queries << t
      puts "result: #{result.to_h}, t: %.3fs" % t
      sleep 0.1
    end
  end
end

threads.each(&:join)

t_elapsed = Time.now - t_start
puts "t_elapsed: %.3fs" % t_elapsed
adapter.stop
puts "t.avg: %.3fs" % (t_queries.sum / t_queries.count)
