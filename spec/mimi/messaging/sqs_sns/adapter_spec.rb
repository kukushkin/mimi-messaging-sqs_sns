# frozen_string_literal: true

RSpec.describe Mimi::Messaging::SQS_SNS::Adapter do
  subject { described_class }
  it { is_expected.to be_a(Class) }
  it { is_expected.to be < Mimi::Messaging::Adapters::Base }


  context "instance" do
    subject { described_class.new(mq_adapter: "sqs_sns") }

    it { is_expected.to respond_to(:start) }
    it { is_expected.to respond_to(:stop) }
    it { is_expected.to respond_to(:command) }
    it { is_expected.to respond_to(:query) }
    it { is_expected.to respond_to(:event) }
    it { is_expected.to respond_to(:start_request_processor) }
    it { is_expected.to respond_to(:start_event_processor) }
    it { is_expected.to respond_to(:start_event_processor_with_queue) }
    it { is_expected.to respond_to(:stop_all_processors) }
  end # instance
end
