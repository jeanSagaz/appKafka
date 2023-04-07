using Adapters.Extensions;
using Adapters.Models;
using Adapters.Producer;
using Business.Models;
using Business.Services;
using Core.MessageBus;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using System.Diagnostics;

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

            string serviceName = "KafkaConsumerWorkerService";
            string serviceVersion = "1.0.0";

            services.AddOpenTelemetry()
                .WithTracing(tracingProviderBuilder =>
                {
                    //builder.AddConsoleExporter();
                    tracingProviderBuilder.AddJaegerExporter(o =>
                    {
                        //o.AgentHost = "jaeger";
                        o.AgentPort = 6831; // use port number here
                    });
                    tracingProviderBuilder.AddSource(serviceName);
                    tracingProviderBuilder.SetResourceBuilder(
                        ResourceBuilder.CreateDefault()
                        .AddService(serviceName: serviceName, serviceVersion: serviceVersion));
                    tracingProviderBuilder.AddHttpClientInstrumentation();
                    tracingProviderBuilder.AddAspNetCoreInstrumentation();
                })
                .WithMetrics(metricsProviderBuilder =>
                {
                    metricsProviderBuilder.AddConsoleExporter();
                    metricsProviderBuilder.SetResourceBuilder(
                    ResourceBuilder.CreateDefault()
                        .AddService(serviceName: serviceName, serviceVersion: serviceVersion));
                    metricsProviderBuilder.AddHttpClientInstrumentation();
                    metricsProviderBuilder.AddAspNetCoreInstrumentation();
                });
                //.StartWithHost();

            services.AddSingleton(sp => new ActivitySource(serviceName, serviceVersion));
        }
    }
}
