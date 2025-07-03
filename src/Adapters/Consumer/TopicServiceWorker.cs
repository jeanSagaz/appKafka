using Adapters.Configurations;
using Adapters.Consumer.Enums;
using Adapters.Extensions;
using Adapters.Serialization;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Net;

namespace Adapters.Consumer
{
    public abstract class TopicServiceWorker<TKey, TValue> : TopicServiceWorkerBase
    {
        private int _connectMaxAttempts = 3;
        private readonly bool _enableDeserializer;
        private readonly string _host;
        protected readonly ActivitySource _activitySource;

        public TopicServiceWorker(ILogger? logger,
            string host,
            string topic,
            string groupId,
            ActivitySource activitySource,
            bool? enableDeserializer)
        : base(logger, host, topic, groupId)
        {
            _host = host;
            _enableDeserializer = enableDeserializer ?? false;
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
                _logger?.LogInformation($"Key: {consumeResult.Message.Key} | Value: {consumeResult.Message.Value}");
            }
            catch (Exception ex)
            {
                // postReceiveAction = PostConsumeAction.Requeue;
                _logger?.LogError(ex, "Error in 'TryGetKeyAndValue'");
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
                        _logger?.LogError(ex, $"Exception on processing topic {_topic}");
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
                    _logger?.LogError(ex, "Key Message rejected during desserialization");
                }

                try
                {
                    consumerBuilder.SetValueDeserializer(new CustomDeserializer<TValue>());
                }
                catch (Exception ex)
                {
                    postReceiveAction = PostConsumeAction.Reject;
                    _logger?.LogError(ex, "Value Message rejected during serialization");
                }
            }

            return consumerBuilder
                .SetLogHandler(new HandlerConfiguration().SetLogHandler)
                .SetErrorHandler(new HandlerConfiguration().SetErrorHandler)
                .SetPartitionsAssignedHandler(new HandlerConfiguration().SetPartitionsAssignedHandler);
        }

        private async Task ProducerAsync<TKey, TValue>(string topic, TKey? key, TValue? value)
        {
            try
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = _host,
                    ClientId = Dns.GetHostName(),
                    // Set to true if you don't want to reorder messages on retry
                    EnableIdempotence = true,
                    // retry settings:
                    // Receive acknowledgement from all sync replicas
                    Acks = Acks.All,
                    // Number of times to retry before giving up
                    MessageSendMaxRetries = 3,
                    // Duration to retry before next attempt
                    RetryBackoffMs = 1000,
                };

                var headers = new Dictionary<string, string>();
                headers["correlation.id"] = Guid.NewGuid().ToString();
                headers["x-death"] = "1";
                headers["topic"] = topic;

                if (key is null)
                {
                    var producer = new ProducerBuilder<Null, TValue>(config)
                        .SetValueSerializer(new CustomSerializer<TValue>())
                        .SetErrorHandler((prod, error) =>
                        {
                            if (error.IsFatal)
                            {
                                _logger?.LogError($"Kafka fatal error: {error.Reason}");
                            }

                            _logger?.LogWarning($"Kafka error: {error.Reason}");
                        })
                        .SetStatisticsHandler((_, json) =>
                        {
                            _logger?.LogInformation($"Set statistics handler producer worker json: {json}");
                        })
                        .SetLogHandler((_, log) =>
                        {
                            _logger?.LogInformation($"Kafka log: {log.Message}");
                        })
                        .Build();

                    var result = await producer.ProduceAsync(topic, new Message<Null, TValue>
                    {
                        Value = value,
                        Headers = headers.DictionaryToHeader()
                    });

                    if (result.Status != PersistenceStatus.Persisted)
                    {
                        // delivery might have failed after retries. This message requires manual processing.
                        _logger?.LogWarning($"ERROR: Message not ack'd by all brokers (value: '{value}'). Delivery status: {result.Status}");
                    }
                }
                else
                {
                    var producer = new ProducerBuilder<TKey, TValue>(config)
                        .SetValueSerializer(new CustomSerializer<TValue>())
                        .SetKeySerializer(new CustomSerializer<TKey>())
                        .Build();

                    var result = await producer.ProduceAsync(topic, new Message<TKey, TValue>
                    {
                        Key = key,
                        Value = value,
                        Headers = headers.DictionaryToHeader()
                    });

                    if (result.Status != PersistenceStatus.Persisted)
                    {
                        // delivery might have failed after retries. This message requires manual processing.
                        _logger?.LogWarning($"ERROR: Message not ack'd by all brokers (value: '{value}'). Delivery status: {result.Status}");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error in 'ProducerAsync'");
            }

            await Task.CompletedTask;
        }

        protected override async Task BuildConsumer(CancellationToken cancellationToken)
        {
            _ = Task.Factory.StartNew(async () =>
            {
                try
                {
                    PostConsumeAction postReceiveAction;
                    var attempts = 0;

                    var consumerBuilder = GetConsumerBuilder(out postReceiveAction);
                    using var consumer = consumerBuilder.Build();
                    consumer.Subscribe($"{_topic}-topic");

                    _logger?.LogInformation($"Kafka consumer topic {_topic}-topic loop started...\n");
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        ConsumeResult<TKey, TValue> consumeResult = new ConsumeResult<TKey, TValue>();

                        try
                        {
                            consumeResult = consumer.Consume(cancellationToken);
                            if (consumeResult.IsPartitionEOF) continue;
                            
                            _logger?.LogInformation($"Kafka consumer topic {_topic}-topic worker running at: {DateTimeOffset.Now}");

                            await Receive(consumeResult, consumer, postReceiveAction);
                        }
                        catch (OperationCanceledException ex)
                        {
                            _logger?.LogWarning(ex, $"Kafka consumer topic {_topic}-topic operation canceled: {ex.Message}");
                            continue;
                        }
                        catch (ConsumeException ex)
                        {
                            // Consumer errors should generally be ignored (or logged) unless fatal.
                            _logger?.LogWarning(ex, $"Kafka consumerException topic {_topic}-topic error: {ex.Error.Reason}");

                            if (ex.Error.IsFatal)
                            {
                                // https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#fatal-consumer-errors
                                await ProducerAsync<TKey, TValue>($"{_topic}-deadletter-topic", consumeResult.Message.Key, consumeResult.Message.Value);

                                Commit(consumer, consumeResult);
                                break;
                            }

                            attempts = await RetryTopic(consumer, consumeResult, attempts);
                        }
                        catch (Exception ex)
                        {
                            _logger?.LogError(ex, $"Kafka consumer topic {_topic}-topic exception error");
                            //throw;

                            attempts = await RetryTopic(consumer, consumeResult, attempts);
                        }
                    }

                    consumer.Close();
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, $"Kafka consumer topic {_topic}-topic will be restarted due to non-retryable");
                }

            }, cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            await Task.CompletedTask;
        }

        private async Task<int> RetryTopic(IConsumer<TKey, TValue> consumer, ConsumeResult<TKey, TValue> consumeResult, int attempts)
        {
            attempts++;
            if (attempts <= _connectMaxAttempts)
            {
                consumer.Seek(consumeResult.TopicPartitionOffset);
                return attempts;
            }

            await ProducerAsync<TKey, TValue>($"{_topic}-deadletter-topic", consumeResult.Message.Key, consumeResult.Message.Value);

            Commit(consumer, consumeResult);

            return 0;
        }

        private void Commit(IConsumer<TKey, TValue> consumer, ConsumeResult<TKey, TValue> consumeResult)
        {
            consumer.Commit(consumeResult);
            consumer.StoreOffset(consumeResult.TopicPartitionOffset);
        }
    }
}
