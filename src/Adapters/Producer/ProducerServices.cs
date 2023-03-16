using Adapters.Configurations;
using Adapters.Extensions;
using Adapters.Serialization;
using Confluent.Kafka;
using Core.MessageBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Adapters.Producer
{
    public class ProducerServices : IProducerServices
    {
        private readonly ILogger<ProducerServices> _logger;
        private readonly string _bootstrapserver;

        public ProducerServices(ILogger<ProducerServices> logger,
            IConfiguration config)
        {
            _logger = logger;
            _bootstrapserver = config.GetSection("KafkaConfigurations:Host").Value;
        }

        public async Task ProducerAsync<TKey, TValue>(string topic, TKey? key, TValue value, bool enableKeySerializer = false, bool enableValueSerializer = false)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = _bootstrapserver,
            };

            var headers = new Dictionary<string, string>();
            headers["kafkaId"] = Guid.NewGuid().ToString();
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

            await Task.CompletedTask;
        }

        public async Task ProducerAsync<TValue>(string topic, TValue value, bool enableKeySerializer = false, bool enableValueSerializer = false)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = _bootstrapserver,
            };

            var headers = new Dictionary<string, string>();
            headers["kafkaId"] = Guid.NewGuid().ToString();
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

            await Task.CompletedTask;
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
