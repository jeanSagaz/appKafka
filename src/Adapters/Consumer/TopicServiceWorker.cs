using Adapters.Configurations;
using Adapters.Consumer.Enums;
using Adapters.Extensions;
using Adapters.Serialization;
using Confluent.Kafka;
using Core.MessageBus;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace Adapters.Consumer
{
    public abstract class TopicServiceWorker<TKey, TValue> : TopicServiceWorkerBase
    {
        private int _attempts = 0;
        private int _connectMaxAttempts = 3;
        private readonly bool _enableDeserializer;
        private readonly IProducerService _producerService;
        protected readonly ActivitySource _activitySource;

        public TopicServiceWorker(ILogger logger,
            string host,
            string topic,
            string groupId,
            IProducerService producerService,
            ActivitySource activitySource,
            bool? enableDeserializer)
        : base(logger, host, topic, groupId)
        {
            _enableDeserializer = enableDeserializer ?? false;
            _producerService = producerService;
            _activitySource = activitySource;
        }

        protected abstract Task<PostConsumeAction> Dispatch(Activity? receiveActivity, TKey? key, TValue value);

        private PostConsumeAction TryGetKeyAndValue(ConsumeResult<TKey, TValue> consumeResult, out TKey? key, out TValue? value)
        {
            if (consumeResult is null) throw new ArgumentNullException(nameof(consumeResult));
            PostConsumeAction postReceiveAction = PostConsumeAction.None;

            key = default;
            value = default;

            try
            {
                key = consumeResult.Message.Key;
                value = consumeResult.Message.Value;
                _logger.LogInformation($"Key: {consumeResult.Message.Key} | Value: {consumeResult.Message.Value}");
            }
            catch (Exception ex)
            {
                // postReceiveAction = PostConsumeAction.Requeue;
                _logger.LogError(ex, "Error in 'TryGetKeyAndValue'");
                throw;
            }

            return postReceiveAction;
        }

        private async Task Receive(ConsumeResult<TKey, TValue> consumeResult, IConsumer<TKey, TValue> consumer, PostConsumeAction postReceiveAction)
        {
            using Activity receiveActivity = this._activitySource.SafeStartActivity("topicServiceWorker.receive", ActivityKind.Consumer);

            if (Activity.Current != null)
                receiveActivity?.SetParentId(Activity.Current.TraceId, Activity.Current.SpanId, ActivityTraceFlags.Recorded);

            receiveActivity?.AddTag("messaging.partition", consumeResult.TopicPartition.Partition.Value);
            receiveActivity?.AddTag("messaging.system", "kafka");
            receiveActivity?.AddTag("messaging.destination_kind", "topic");
            receiveActivity?.AddTag("messaging.topic", this._topic);

            if (postReceiveAction == PostConsumeAction.None)
            {
                postReceiveAction = TryGetKeyAndValue(consumeResult, out TKey? key, out TValue? value);

                if (postReceiveAction == PostConsumeAction.None)
                {
                    try
                    {
                        var headers = consumeResult.Message.Headers.HeaderToDictionary();
                        if (headers.ContainsKey("correlation.id"))                        
                            receiveActivity?.AddTag("correlation.id", headers["correlation.id"]);                        

                        if (value is not null)
                            postReceiveAction = await Dispatch(receiveActivity, key, value);
                    }
                    catch (Exception ex)
                    {
                        postReceiveAction = PostConsumeAction.Reject;
                        _logger.LogError(ex, $"Exception on processing topic {_topic}");
                        throw;
                    }
                }
            }

            switch (postReceiveAction)
            {
                case PostConsumeAction.None: throw new InvalidOperationException("None is unsupported");
                case PostConsumeAction.Commit:
                    Commit(consumer, consumeResult);
                    break;
                case PostConsumeAction.Reject:
                case PostConsumeAction.Requeue:
                    consumer.Seek(consumeResult.TopicPartitionOffset);
                    break;
            }

            receiveActivity?.SetEndTime(DateTime.UtcNow);
        }

        private ConsumerBuilder<TKey, TValue> GetConsumerBuilder(out PostConsumeAction postReceiveAction)
        {
            postReceiveAction = PostConsumeAction.None;
            var consumerBuilder = new ConsumerBuilder<TKey, TValue>(_consumerConfig);

            if (_enableDeserializer)
            {
                try
                {
                    consumerBuilder.SetKeyDeserializer(new CustomDeserializer<TKey>());
                }
                catch (Exception ex)
                {
                    postReceiveAction = PostConsumeAction.Reject;
                    _logger.LogError(ex, "Key Message rejected during desserialization");
                }

                try
                {
                    consumerBuilder.SetValueDeserializer(new CustomDeserializer<TValue>());
                }
                catch (Exception ex)
                {
                    postReceiveAction = PostConsumeAction.Reject;
                    _logger.LogError(ex, "Value Message rejected during serialization");
                }
            }

            return consumerBuilder
                .SetLogHandler(new HandlerConfiguration().SetLogHandler)
                .SetErrorHandler(new HandlerConfiguration().SetErrorHandler)
                .SetPartitionsAssignedHandler(new HandlerConfiguration().SetPartitionsAssignedHandler);
        }

        protected override async Task BuildConsumer(CancellationToken cancellationToken)
        {
            _ = Task.Factory.StartNew(async () =>
            {
                try
                {
                    PostConsumeAction postReceiveAction;

                    var consumerBuilder = GetConsumerBuilder(out postReceiveAction);
                    using var consumer = consumerBuilder.Build();
                    consumer.Subscribe($"{_topic}-topic");

                    _logger.LogInformation($"Kafka consumer topic {_topic}-topic loop started...\n");
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        ConsumeResult<TKey, TValue> consumeResult = new ConsumeResult<TKey, TValue>();

                        try
                        {
                            consumeResult = consumer.Consume(cancellationToken);
                            if (consumeResult.IsPartitionEOF) continue;
                            
                            _logger.LogInformation($"Kafka consumer topic {_topic}-topic worker running at: {DateTimeOffset.Now}");

                            await Receive(consumeResult, consumer, postReceiveAction);
                        }
                        catch (OperationCanceledException ex)
                        {
                            _logger.LogWarning(ex, $"Kafka consumer topic {_topic}-topic operation canceled: {ex.Message}");
                            continue;
                        }
                        catch (ConsumeException ex)
                        {
                            // Consumer errors should generally be ignored (or logged) unless fatal.
                            _logger.LogWarning(ex, $"Kafka consumerException topic {_topic}-topic error: {ex.Error.Reason}");

                            if (ex.Error.IsFatal)
                            {
                                // https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#fatal-consumer-errors
                                await _producerService.ProducerAsync<TKey, TValue>($"{_topic}-deadletter-topic", consumeResult.Message.Key, consumeResult.Message.Value, _enableDeserializer, _enableDeserializer);

                                Commit(consumer, consumeResult);
                                break;
                            }

                            await RetryTopic(consumer, consumeResult);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, $"Kafka consumer topic {_topic}-topic exception error");
                            //throw;

                            await RetryTopic(consumer, consumeResult);
                        }
                    }

                    consumer.Close();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Kafka consumer topic {_topic}-topic will be restarted due to non-retryable");
                }

            }, cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            await Task.CompletedTask;
        }

        private async Task RetryTopic(IConsumer<TKey, TValue> consumer, ConsumeResult<TKey, TValue> consumeResult)
        {            
            if (_attempts <= _connectMaxAttempts)
            {
                consumer.Seek(consumeResult.TopicPartitionOffset);
                _attempts++;
                return;
            }

            await _producerService.ProducerAsync<TKey, TValue>($"{_topic}-deadletter-topic", consumeResult.Message.Key, consumeResult.Message.Value, _enableDeserializer, _enableDeserializer);

            Commit(consumer, consumeResult);
        }

        private void Commit(IConsumer<TKey, TValue> consumer, ConsumeResult<TKey, TValue> consumeResult)
        {
            consumer.Commit(consumeResult);
            consumer.StoreOffset(consumeResult.TopicPartitionOffset);
        }
    }
}
