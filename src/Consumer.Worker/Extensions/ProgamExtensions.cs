using Adapters.Extensions;
using Adapters.Models;
using Adapters.Producer;
using Business.Models;
using Business.Services;
using Core.MessageBus;

namespace Consumer.Worker.Extensions
{
    public static class ProgamExtensions
    {
        public static void Configure(HostBuilderContext hostContext, IServiceCollection services)
        {
            //services.AddHostedService<ConsumerWorker>();

            IConfiguration configuration = hostContext.Configuration;
            var configurations = configuration.GetSection("KafkaConfigurations").Get<KafkaConfigurations>();

            services.AddTransient<IProducerServices, ProducerServices>();
            services.AddTransient<ExecuteAnythingService>();

            // if (hostContext.CanRun("customer"))
            services.AddAsyncTopicConsumer<ExecuteAnythingService, ExecuteAnythingRequest>(host: configurations.Host,
                    topic: "authorizer-topic",
                    groupId: "authorizer-group-0",
                    functionToExecute: async (svc, value) => await svc.ExecuteAnything(value),
                    enableDeserializer: true);

            //services.AddAsyncTopicConsumer<ExecuteAnythingService, string, string>(host: configurations.Host,
            //    topic: "authorizer-topic",
            //    groupId: "authorizer-group-0",
            //    functionToExecute: async (svc, key, value) => await svc.ExecuteAnything(key, value));

            services.AddAsyncTopicConsumer<ExecuteAnythingService, string, string>(host: configurations.Host,
                topic: "payment-topic",
                groupId: "payment-group-0",
                functionToExecute: async (svc, key, value) => await svc.ExecuteAnything(key, value));

            //services.AddHealthChecks();
            //services
            //    .AddHealthChecksUI();
            //.AddHealthChecksUI(setupSettings: setup =>
            //{
            //setup.AddHealthCheckEndpoint("hc", "http://localhost:8001/hc");
            //setup.AddHealthCheckEndpoint("endpoint2", "http://remoteendpoint:9000/healthz");
            //setup.AddWebhookNotification("webhook1", uri: "http://httpbin.org/status/200?code=ax3rt56s", payload: "{...}");
            //})
            //.AddInMemoryStorage();
        }
    }
}
