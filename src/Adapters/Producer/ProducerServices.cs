using Adapters.Configurations;
using Adapters.Extensions;
using Adapters.Serialization;
using Confluent.Kafka;
using Core.MessageBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Transactions;

namespace Adapters.Producer
{
    public class ProducerServices : IProducerServices
    {
        private readonly ILogger<ProducerServices> _logger;
        private readonly string _bootstrapserver;
        private readonly ActivitySource _activitySource;

        public ProducerServices(ILogger<ProducerServices> logger,
            IConfiguration config,
            ActivitySource activitySource)
        {
            _logger = logger;
            _bootstrapserver = config.GetSection("KafkaConfigurations:Host").Value;
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
                    BootstrapServers = _bootstrapserver,
                };

                var headers = new Dictionary<string, string>();
                headers["correlation.id"] = transactionId;
                headers["x-death"] = "1";
                headers["topic"] = topic;

                var producerBuilder = GetProducerBuilder<TKey, TValue>(config, enableKeySerializer, enableValueSerializer);
                var producer = producerBuilder.Build();

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
                    BootstrapServers = _bootstrapserver,
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
