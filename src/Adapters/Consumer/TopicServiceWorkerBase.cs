﻿using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Adapters.Consumer
{
    public abstract class TopicServiceWorkerBase : BackgroundService
    {
        protected readonly ILogger? _logger;
        protected readonly ConsumerConfig _consumerConfig;
        protected string _topic { get; }

        protected TopicServiceWorkerBase(ILogger? logger,
            string host,
            string topic,
            string groupId)
        {
            _logger = logger;
            _topic = topic;

            _consumerConfig = new ConsumerConfig
            {
                BootstrapServers = host,
                GroupId = groupId,
                // Read messages from start if no commit exists.
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true,
                EnableAutoCommit = false,                
                EnableAutoOffsetStore = false,
                MaxPollIntervalMs = 10000
            };
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await this.BuildConsumer(stoppingToken);

            while (!stoppingToken.IsCancellationRequested)
            {
                _logger?.LogInformation("Kafka worker running at: {time} - Topic: {topic}", DateTimeOffset.Now, _topic);
                await Task.Delay(1000, stoppingToken);
            }

            // or
            // await Task.CompletedTask;
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            await base.StopAsync(cancellationToken);
            _logger?.LogInformation($"Kafka worker topic {_topic} stopAsync.");
        }

        public override void Dispose()
        {            
            base.Dispose();
            _logger?.LogInformation($"Kafka worker topic {_topic} dispose.");
        }

        protected abstract Task BuildConsumer(CancellationToken stoppingToken);
    }
}
