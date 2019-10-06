# mimi-messaging-sqs_sns

AWS SQS/SNS adapter for [mimi-messaging](https://github.com/kukushkin/mimi-messaging).

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'mimi-messaging-sqs_sns'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install mimi-messaging-sqs_sns

## Usage

```ruby
require "mimi/messaging"
require "mimi/messaging/sqs_sns"

Mimi::Messaging.configure(
  mq_adapter: "sqs_sns",
  ...
)

Mimi::Messaging.start
```


## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/kukushkin/mimi-messaging-sqs_sns. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the mimi-messaging-sqs_sns project’s codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/kukushkin/mimi-messaging-sqs_sns/blob/master/CODE_OF_CONDUCT.md).
