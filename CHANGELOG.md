# Changelog

## UNRELEASED

## v0.8.2

* [#7](https://github.com/kukushkin/mimi-messaging-sqs_sns/pull/7)
  * Fixed an issue in the error processing, which could potentially crash the consumer threads

## v0.8.1

* [#5](https://github.com/kukushkin/mimi-messaging-sqs_sns/pull/5)
  * Refactored worker pool based message processing and error handling

## v0.8.0

* [#3](https://github.com/kukushkin/mimi-messaging-sqs_sns/pull/3)
  * Added a worker pool:
    * now processing of messages from a single queue can be done in multiple parallel threads (workers)
    * the worker threads are shared across all message consumers which read messages from different queues
    * the size of the worker pool is limited, to have at most `mq_worker_pool_max_threads` processing messages in parallel
    * `mq_worker_pool_min_threads` determines the target minimal number of threads in the pool, waiting for new messages
    * `mq_worker_pool_max_backlog` controls how many messages which are read from SQS queues can be put in the worker pool backlog; if a new message is read from SQS queue and the backlog is full, this message is NACK-ed (put back into SQS queue for the other consumers to process)
  * Improved thread-safety of the adapter: reply consumer, TimeoutQueue#pop

## v0.7.0

* [#1](https://github.com/kukushkin/mimi-messaging-sqs_sns/pull/1)
  * Added KMS support for creating queues/topics with sever-side encryption enabled
  * Optimized stopping of the adapter: stopping all consumers in parallel


## v0.6.x

* Basic functionality implemented, see missing features in [TODO](TODO.md)
