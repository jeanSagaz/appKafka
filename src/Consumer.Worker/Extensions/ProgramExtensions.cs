using Adapters.Extensions;
using Adapters.Models;
using Adapters.Producer;
using Business.Models;
using Business.Services;
using Core.MessageBus;

namespace Consumer.Worker.Extensions
{
    public static class ProgramExtensions
    {
        public static void Configure(HostBuilderContext hostContext, IServiceCollection services)
        {
            //services.AddHostedService<ConsumerWorker>();

            IConfiguration configuration = hostContext.Configuration;

            var appSettings = new AppSettings();
            configuration.Bind(appSettings);
            services.AddSingleton(appSettings);

            if (appSettings?.KafkaConfigurations is null)
                throw new ArgumentNullException("Kafka configurations cannot be null");

            services.AddTransient<IProducerService, ProducerService>();
            services.AddTransient<ExecuteAnythingService>();

            services.AddAsyncTopicConsumer<ExecuteAnythingService, ExecuteAnythingRequest>(host: appSettings.KafkaConfigurations.Host,
                    topic: "authorizer",
                    groupId: "authorizer-group-0",
                    functionToExecute: async (svc, value) => await svc.ExecuteAnything(value),
                    enableDeserializer: true);

            services.AddAsyncTopicConsumer<ExecuteAnythingService, ExecuteAnythingRequest>(host: appSettings.KafkaConfigurations.Host,
                topic: "exception",
                groupId: "exception-group-0",
                functionToExecute: async (svc, value) => await svc.ThrowException(value));

            services.AddAsyncTopicConsumer<ExecuteAnythingService, string, string>(host: appSettings.KafkaConfigurations.Host,
                topic: "payment",
                groupId: "payment-group-0",
                functionToExecute: async (svc, key, value) => await svc.ExecuteAnything(key, value));

            services.AddOpenTelemetry(configuration);
        }

        public static void Configure(this IServiceCollection services, IConfiguration configuration)
        {
            //services.AddHostedService<ConsumerWorker>();

            var appSettings = new AppSettings();
            configuration.Bind(appSettings);
            services.AddSingleton(appSettings);

            if (appSettings?.KafkaConfigurations is null)
                throw new ArgumentNullException("Kafka configurations cannot be null");

            services.AddTransient<IProducerService, ProducerService>();
            services.AddTransient<ExecuteAnythingService>();

            services.AddAsyncTopicConsumer<ExecuteAnythingService, ExecuteAnythingRequest>(host: appSettings.KafkaConfigurations.Host,
                    topic: "authorizer",
                    groupId: "authorizer-group-0",
                    functionToExecute: async (svc, value) => await svc.ExecuteAnything(value),
                    enableDeserializer: true);

            services.AddAsyncTopicConsumer<ExecuteAnythingService, ExecuteAnythingRequest>(host: appSettings.KafkaConfigurations.Host,
                topic: "exception",
                groupId: "exception-group-0",
                functionToExecute: async (svc, value) => await svc.ThrowException(value));

            services.AddAsyncTopicConsumer<ExecuteAnythingService, string, string>(host: appSettings.KafkaConfigurations.Host,
                topic: "payment",
                groupId: "payment-group-0",
                functionToExecute: async (svc, key, value) => await svc.ExecuteAnything(key, value));

            services.AddOpenTelemetry(configuration);
        }
    }
}
