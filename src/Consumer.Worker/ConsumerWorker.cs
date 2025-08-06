using Adapters.Models;
using Adapters.Serialization;
using Business.Models;
using Confluent.Kafka;

namespace Consumer.Worker
{
    public class ConsumerWorker : BackgroundService
    {
        private readonly ILogger<ConsumerWorker> _logger;
        private readonly AppSettings _kafkaConfigurations;
        private readonly IConfiguration _config;
        private readonly string _host;
        private readonly string _topic;

        public ConsumerWorker(ILogger<ConsumerWorker> logger,
            AppSettings appSettings)
            //IConfiguration config)
        {
            _logger = logger;
            _kafkaConfigurations = appSettings;
            //_config = config;
            //_host = _config.GetSection("KafkaConfigurations:Host").Value;
            //_topic = _config.GetSection("KafkaConfigurations:Topic").Value;
            _host = _kafkaConfigurations.KafkaConfigurations.Host;
            _topic = "authorizer-topic";
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _host,
                GroupId = $"{_topic}-group-0",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            //using (var consumer = new ConsumerBuilder<string, string>(config)
            using (var consumer = new ConsumerBuilder<Null, Value>(config)
                .SetValueDeserializer(new CustomDeserializer<Value>())
                .SetLogHandler((_, logMessage) => _logger.LogInformation("Kafka log: {Message}", logMessage.Message))
                .SetErrorHandler((_, error) =>
                {
                    if (error.IsFatal)
                    {
                        _logger.LogError("Kafka fatal error: {Reason}", error.Reason);
                        throw new KafkaException(error);
                    }
                    _logger.LogWarning("Kafka error: {Reason}", error.Reason);
                })
                .SetPartitionsAssignedHandler((c, partitions) => 
                {
                    _logger.LogInformation("Kafka log: Assigned partitions: [{Partitions}]", string.Join(", ", partitions));
                })
                .Build())
            {
                consumer.Subscribe(_topic);
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        _logger.LogInformation("Kafka consumer loop started...\n");
                        var cr = consumer.Consume(stoppingToken);
                        _logger.LogInformation($"Key: {cr.Message.Key} | Value: {cr.Message.Value.Input}");
                        _logger.LogInformation("Consumer worker running at: {time}", DateTimeOffset.Now);
                    }
                    catch (OperationCanceledException oce)
                    {
                        _logger.LogWarning($"Consume canceled: {oce.Message}");
                        continue;
                    }
                    catch (ConsumeException e)
                    {
                        // Consumer errors should generally be ignored (or logged) unless fatal.
                        _logger.LogWarning($"Consume error: {e.Error.Reason}");

                        if (e.Error.IsFatal)
                        {
                            // https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#fatal-consumer-errors
                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Kafka consumer will be restarted due to non-retryable exception: {Message}", ex.Message);
                        throw;
                    }
                }
            }

            await Task.CompletedTask;
        }
    }
}