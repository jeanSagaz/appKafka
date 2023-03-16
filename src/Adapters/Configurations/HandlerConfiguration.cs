using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Adapters.Configurations
{
    public class HandlerConfiguration
    {
        private readonly ILogger _logger;

        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddConsole();
            builder.AddDebug();
        });

        public HandlerConfiguration()
        {
            _logger = loggerFactory.CreateLogger<HandlerConfiguration>();
        }

        public void SetErrorHandler<TKey, TValue>(IProducer<TKey, TValue> producer, Error error)
        {
            if (error.IsFatal)
            {
                _logger.LogError("Kafka producer fatal error: {Reason}", error.Reason);
            }
            _logger.LogWarning("Kafka producer error: {Reason}", error.Reason);
        }

        public void SetStatisticsHandler<TKey, TValue>(IProducer<TKey, TValue> producer, string json)
        {
            _logger.LogInformation("Set statistics handler producer worker json: {json}", json);
        }

        public void SetLogHandler<TKey, TValue>(IProducer<TKey, TValue> producer, LogMessage logMessage)
        {
            _logger.LogInformation("Kafka producer log: {Message}", logMessage.Message);
        }

        public void SetErrorHandler<TKey, TValue>(IConsumer<TKey, TValue> consumer, Error error)
        {
            if (error.IsFatal)
            {
                _logger.LogError("Kafka consumer fatal error: {Reason}", error.Reason);
                throw new KafkaException(error);
            }
            _logger.LogWarning("Kafka consumer error: {Reason}", error.Reason);
        }

        public void SetPartitionsAssignedHandler<TKey, TValue>(IConsumer<TKey, TValue> consumer, List<TopicPartition> topicPartitions)
        {
            _logger.LogInformation("Kafka consumer log: Assigned partitions: [{Partitions}]", string.Join(", ", topicPartitions));
        }

        public void SetLogHandler<TKey, TValue>(IConsumer<TKey, TValue> consumer, LogMessage logMessage)
        {
            _logger.LogInformation("Kafka consumer log: {Message}", logMessage.Message);
        }
    }
}
