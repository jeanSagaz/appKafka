using Adapters.Extensions;
using Adapters.Serialization;
using Confluent.Kafka;
using Producer.Worker.Models;
using System.Net;
using System.Text;
using System.Text.Json;

namespace Producer.Worker
{
    public class ProducerWorker : BackgroundService
    {
        private readonly ILogger<ProducerWorker> _logger;
        private readonly IConfiguration _config;
        private readonly string _host;
        private readonly string _topic;

        //private static readonly ObjectSerializer<T> serializer = new ObjectSerializer<T>();

        public ProducerWorker(ILogger<ProducerWorker> logger,
            IConfiguration config)
        {
            _logger = logger;
            _config = config;
            _host = _config.GetSection("KafkaConfigurations:Host").Value;
            //_topic = _config.GetSection("KafkaConfigurations:Topic").Value;
            _topic = "authorizer-topic";
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
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

                using (var producer = new ProducerBuilder<string, string>(config)
                //using (var producer = new ProducerBuilder<Null, ExecuteAnythingRequest>(config)
                    //.SetValueSerializer(new CustomSerializer<ExecuteAnythingRequest>())
                    //.SetKeySerializer(Serializers.Int64)                    
                    .SetErrorHandler((producer, error) =>
                    {
                        // Write(this.HttpContext, "--Kafka Producer Error: " + error.Reason);                        
                        if (error.IsFatal)
                        {
                            _logger.LogError("Kafka fatal error: {Reason}", error.Reason);
                        }
                        _logger.LogWarning("Kafka error: {Reason}", error.Reason);
                    })
                    .SetStatisticsHandler((_, json) =>
                    {
                        _logger.LogInformation("Set statistics handler producer worker json: {json}", json);
                    })
                    .SetLogHandler((_, logMessage) =>
                    {
                        _logger.LogInformation("Kafka log: {Message}", logMessage.Message);
                    })
                    .Build())
                {

                    var headers = new Dictionary<string, string>();
                    headers["correlation.id"] = Guid.NewGuid().ToString();
                    headers["x-death"] = "1";
                    headers["topic"] = _topic;

                    int i = 0;
                    //while (!stoppingToken.IsCancellationRequested)
                    //{
                        var result = await producer.ProduceAsync(_topic,
                            new Message<string, string>
                            {
                                //Key = $"KEY_{i}",
                                //Value = Guid.NewGuid().ToString()
                                Key = "Jean",
                                Value = "Jean",
                                Headers = headers.DictionaryToHeader()
                            });

                        //var result = await producer.ProduceAsync(_topic,
                        //    new Message<Null, ExecuteAnythingRequest>
                        //    {
                        //        Key = $"KEY_{i}",
                        //        Value = new ExecuteAnythingRequest()
                        //        {
                        //            Name = $"Jean - {i}"
                        //        },
                        //        Headers = headers.DictionaryToHeader()
                        //    });

                        if (result.Status != PersistenceStatus.Persisted)
                        {
                            // delivery might have failed after retries. This message requires manual processing.
                            Console.WriteLine($"ERROR: Message not ack'd by all brokers (value: 'KEY_{i}'). Delivery status: {result.Status}");
                        }

                        // wait for up to 1 seconds for any inflight messages to be delivered.
                        producer.Flush(TimeSpan.FromSeconds(1));
                        i++;
                        _logger.LogInformation("Producer worker running at: {time}", DateTimeOffset.Now);
                    //}
                }
            }
            catch (ProduceException<Null, string> e)
            {
                Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                _logger.LogError(e, $"Delivery failed: {e.Error.Reason}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "error producer worker");
                throw;
            }

            await Task.CompletedTask;
        }
    }
}