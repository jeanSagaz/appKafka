using Adapters.Configurations;
using Adapters.Consumer.Enums;
using Adapters.Extensions;
using Adapters.Serialization;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Transactions;

namespace Adapters.Consumer
{
    public abstract class TopicServiceWorker<TKey, TValue> : TopicServiceWorkerBase
    {
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
            this._activitySource = activitySource;
        }

        protected abstract Task<PostConsumeAction> Dispatch(Activity? receiveActivity, TKey key, TValue value);

        private PostConsumeAction TryGetKeyAndValue(ConsumeResult<TKey, TValue> consumeResult, out TKey key, out TValue value)
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
            catch (Exception exception)
            {
                postReceiveAction = PostConsumeAction.Requeue;
                _logger?.LogWarning("Message rejected during desserialization {exception}", exception);
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
                postReceiveAction = TryGetKeyAndValue(consumeResult, out TKey key, out TValue value);

                if (postReceiveAction == PostConsumeAction.None)
                {
                    try
                    {
                        var headers = consumeResult.Message.Headers.HeaderToDictionary();
                        if(headers.ContainsKey("correlation.id"))
                        {
                            receiveActivity?.AddTag("Correlation.Id", headers["correlation.id"]);
                        }                            
                        postReceiveAction = await Dispatch(receiveActivity, key, value);
                    }
                    catch (Exception exception)
                    {
                        postReceiveAction = PostConsumeAction.Reject;
                        _logger?.LogError("Exception on processing message {topic} {exception}", _topic, exception);
                    }
                }
            }

            switch (postReceiveAction)
            {
                case PostConsumeAction.None: throw new InvalidOperationException("None is unsupported");
                case PostConsumeAction.Commit:
                    consumer.Commit(consumeResult);
                    consumer.StoreOffset(consumeResult.TopicPartitionOffset);
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
                catch (Exception exception)
                {
                    postReceiveAction = PostConsumeAction.Reject;
                    _logger?.LogError(exception, "Message rejected during desserialization");
                }

                try
                {
                    consumerBuilder.SetValueDeserializer(new CustomDeserializer<TValue>());
                }
                catch (Exception exception)
                {
                    postReceiveAction = PostConsumeAction.Reject;
                    _logger?.LogError(exception, "Message rejected during serialization");
                }
            }

            return consumerBuilder
                .SetLogHandler(new HandlerConfiguration().SetLogHandler)
                .SetErrorHandler(new HandlerConfiguration().SetErrorHandler)
                .SetPartitionsAssignedHandler(new HandlerConfiguration().SetPartitionsAssignedHandler);
        }

        private async Task ProducerAsync<TKey, TValue>(string topic, TKey? key, TValue value)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = _host,
            };

            var headers = new Dictionary<string, string>();
            headers["correlation.id"] = Guid.NewGuid().ToString();
            headers["x-death"] = "1";
            headers["topic"] = topic;

            var producerBuilder = new ProducerBuilder<TKey, TValue>(config);
            var producer = producerBuilder.Build();

            var result = await producer.ProduceAsync($"{topic}-deadletter-topic", new Message<TKey, TValue>
            {
                Key = key,
                Value = value,
                Headers = headers.DictionaryToHeader()
            });

            if (result.Status != PersistenceStatus.Persisted)
            {
                // delivery might have failed after retries. This message requires manual processing.
                _logger.LogWarning($"ERROR: Message not ack'd by all brokers (value: '{value}'). Delivery status: {result.Status}");
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

                    var consumerBuilder = GetConsumerBuilder(out postReceiveAction);
                    using var consumer = consumerBuilder.Build();
                    consumer.Subscribe(_topic);

                    _logger?.LogInformation($"Kafka consumer topic {_topic} loop started...\n");
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        ConsumeResult<TKey, TValue> consumeResult = new ConsumeResult<TKey, TValue>();

                        try
                        {
                            consumeResult = consumer.Consume(cancellationToken);
                            if (consumeResult.IsPartitionEOF)
                            {
                                continue;
                            }
                            _logger?.LogInformation("Kafka consumer topic {topic} worker running at: {time}", _topic, DateTimeOffset.Now);

                            await Receive(consumeResult, consumer, postReceiveAction);
                        }
                        catch (OperationCanceledException ex)
                        {
                            _logger?.LogWarning(ex, $"Kafka consumer topic {this._topic} operation canceled: {ex.Message}");
                            continue;
                        }
                        catch (ConsumeException ex)
                        {
                            // Consumer errors should generally be ignored (or logged) unless fatal.
                            _logger?.LogWarning(ex, $"Kafka consumerException topic {this._topic} error: {ex.Error.Reason}");

                            if (ex.Error.IsFatal)
                            {
                                // https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#fatal-consumer-errors

                                //await ProducerAsync<TKey, TValue>("deadletter-topic", consumeResult.Key, consumeResult.Value);
                                break;
                            }

                            // TODO: TESTAR ESSA PARTE
                            // testar publicar no tópico de retry
                            //consumeResult = consumer.Consume(cancellationToken);
                            //await ProducerAsync<TKey, TValue>("retry-topic", consumeResult.Key, consumeResult.Value);
                            
                            // publica no tópico novamente
                            //consumer.Seek(consumeResult.TopicPartitionOffset);

                            // remove do tópico
                            //consumer.Commit(consumeResult);
                            //consumer.StoreOffset(consumeResult.TopicPartitionOffset);
                        }
                        catch (Exception ex)
                        {
                            _logger?.LogError(ex, $"Kafka consumer topic {this._topic} exception error");
                            throw;
                        }
                    }

                    consumer.Close();

                    _logger?.LogWarning($"Kafka consumer topic {this._topic} send message to unrouted");
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Kafka consumer topic {topic} will be restarted due to non-retryable exception: {Message}", this._topic, ex.Message);
                }

            }, cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            await Task.CompletedTask;
        }
    }
}
