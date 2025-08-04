using Adapters.Configurations;
using Adapters.Extensions;
using Adapters.Models;
using Adapters.Serialization;
using Confluent.Kafka;
using Core.MessageBus;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Net;

namespace Adapters.Producer
{
    public class ProducerService : IProducerService
    {
        private readonly ILogger<ProducerService> _logger;
        private readonly string _host;
        private readonly ActivitySource _activitySource;

        public ProducerService(ILogger<ProducerService> logger,
            AppSettings appSettings,
            ActivitySource activitySource)
        {
            _logger = logger;
            _host = appSettings.KafkaConfigurations.Host;
            _activitySource = activitySource;
        }

        public async Task ProducerAsync<TKey, TValue>(string topic, TKey? key, TValue value, bool enableKeySerializer = false, bool enableValueSerializer = false)
        {
            try
            {
                using Activity currentActivity = _activitySource.SafeStartActivity("kafka.producer", ActivityKind.Producer);
                var transactionId = Guid.NewGuid().ToString();

                currentActivity?.AddTag("correlation.id", transactionId);
                currentActivity?.AddTag("messaging.system", "kafka");
                currentActivity?.AddTag("messaging.destination_kind", "topic");
                currentActivity?.AddTag("messaging.topic", topic);

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
                headers["correlation.id"] = transactionId;
                headers["x-death"] = "1";
                headers["topic"] = topic;

                var producer = GetProducerBuilder<TKey, TValue>(config, enableKeySerializer, enableValueSerializer)
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
                    _logger.LogWarning($"ERROR: Message not ack'd by all brokers (value: '{value}'). Delivery status: {result.Status}");
                }

                currentActivity?.SetEndTime(DateTime.UtcNow);
                await Task.CompletedTask;
            }
            catch(Exception ex)
            {
                _logger.LogError(ex, "Producer error");
                throw;
            }
        }

        public async Task ProducerAsync<TValue>(string topic, TValue value, bool enableKeySerializer = false, bool enableValueSerializer = false)
        {
            try
            {
                using Activity currentActivity = _activitySource.SafeStartActivity("kafka.producer", ActivityKind.Producer);
                var transactionId = Guid.NewGuid().ToString();

                currentActivity?.AddTag("transactionId", transactionId);
                currentActivity?.AddTag("messaging.system", "kafka");
                currentActivity?.AddTag("messaging.destination_kind", "topic");
                currentActivity?.AddTag("messaging.topic", topic);

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
                headers["correlation.id"] = transactionId;
                headers["x-death"] = "1";
                headers["topic"] = topic;

                var producerBuilder = GetNullProducerBuilder<Null, TValue>(config, enableKeySerializer, enableValueSerializer);
                var producer = producerBuilder.Build();

                var result = await producer.ProduceAsync(topic, new Message<Null, TValue>
                {
                    Value = value,
                    Headers = headers.DictionaryToHeader()
                });

                if (result.Status != PersistenceStatus.Persisted)
                {
                    // delivery might have failed after retries. This message requires manual processing.
                    _logger.LogWarning($"ERROR: Message not ack'd by all brokers (value: '{value}'). Delivery status: {result.Status}");
                }

                currentActivity?.SetEndTime(DateTime.UtcNow);
                await Task.CompletedTask;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Producer error");
                throw;
            }
        }

        private ProducerBuilder<TKey, TValue> GetProducerBuilder<TKey, TValue>(ProducerConfig config, bool? enableKeySerializer, bool? enableValueSerializer)
        {
            var producerBuilder = new ProducerBuilder<TKey, TValue>(config);

            if (enableKeySerializer.HasValue && enableKeySerializer.Value)
                producerBuilder.SetKeySerializer(new CustomSerializer<TKey>());

            if (enableValueSerializer.HasValue && enableValueSerializer.Value)
                producerBuilder.SetValueSerializer(new CustomSerializer<TValue>());

            return producerBuilder
                .SetErrorHandler(new HandlerConfiguration().SetErrorHandler)
                .SetStatisticsHandler(new HandlerConfiguration().SetStatisticsHandler)
                .SetLogHandler(new HandlerConfiguration().SetLogHandler);
        }

        private ProducerBuilder<Null, TValue> GetNullProducerBuilder<Null, TValue>(ProducerConfig config, bool? enableKeySerializer, bool? enableValueSerializer)
        {
            var producerBuilder = new ProducerBuilder<Null, TValue>(config);            

            if (enableValueSerializer.HasValue && enableValueSerializer.Value)
                producerBuilder.SetValueSerializer(new CustomSerializer<TValue>());

            return producerBuilder
                .SetErrorHandler(new HandlerConfiguration().SetErrorHandler)
                .SetStatisticsHandler(new HandlerConfiguration().SetStatisticsHandler)
                .SetLogHandler(new HandlerConfiguration().SetLogHandler);
        }
    }
}
