using Adapters.Configurations;
using Adapters.Consumer.Enums;
using Adapters.Extensions;
using Adapters.Serialization;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Adapters.Consumer
{
    public abstract class TopicServiceWorker<TKey, TValue> : TopicServiceWorkerBase
    {
        private readonly bool _enableDeserializer;
        private readonly string _host;

        public TopicServiceWorker(ILogger? logger,
            string host,
            string topic,
            string groupId,
            bool? enableDeserializer)
        : base(logger, host, topic, groupId)
        {
            _host = host;
            _enableDeserializer = enableDeserializer ?? false;
        }

        protected abstract Task<PostConsumeAction> Dispatch(TKey key, TValue value);

        private PostConsumeAction GetKeyAndValue(ConsumeResult<TKey, TValue> consumeResult, out TKey key, out TValue value)
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
            if (postReceiveAction == PostConsumeAction.None)
            {
                postReceiveAction = GetKeyAndValue(consumeResult, out TKey key, out TValue value);

                if (postReceiveAction == PostConsumeAction.None)
                {
                    try
                    {
                        var headers = consumeResult.Message.Headers.HeaderToDictionary();
                        postReceiveAction = await Dispatch(key, value);
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
            headers["kafkaId"] = Guid.NewGuid().ToString();
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
                            //var consumeResult = consumer.Consume(stoppingToken);
                            consumeResult = consumer.Consume(cancellationToken);
                            if (consumeResult.IsPartitionEOF)
                            {
                                continue;
                            }
                            _logger?.LogInformation("Kafka consumer topic {topic} worker running at: {time}", _topic, DateTimeOffset.Now);

                            await Receive(consumeResult, consumer, postReceiveAction);
                        }
                        catch (OperationCanceledException oce)
                        {
                            _logger?.LogWarning("Kafka consumer topic {topic} operation canceled: {Message}", this._topic, oce.Message);
                            continue;
                        }
                        catch (ConsumeException e)
                        {
                            // Consumer errors should generally be ignored (or logged) unless fatal.
                            _logger?.LogWarning($"Kafka consumerException topic {this._topic} error: {e.Error.Reason}");

                            if (e.Error.IsFatal)
                            {
                                // https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#fatal-consumer-errors                            
                                break;
                            }

                            var c = consumer.Consume(cancellationToken);
                            //await ProducerAsync<TKey, TValue>("retry-topic", c.Key, c.Value);
                            consumer.Seek(consumeResult.TopicPartitionOffset);
                        }
                        catch (Exception ex)
                        {
                            _logger?.LogError(ex, $"Kafka consumer topic {this._topic} exception error");
                            throw;
                        }
                    }

                    consumer.Close();

                    _logger?.LogWarning($"Kafka consumer topic {this._topic} send message to un routed");
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
