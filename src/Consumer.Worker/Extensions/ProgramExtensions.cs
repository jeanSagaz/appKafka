using Adapters.Extensions;
using Adapters.Models;
using Adapters.Producer;
using Business.Models;
using Business.Services;
using Core.MessageBus;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Consumer.Worker.Extensions
{
    public static class ProgramExtensions
    {
        public static void Configure(HostBuilderContext hostContext, IServiceCollection services)
        {
            //services.AddHostedService<ConsumerWorker>();

            IConfiguration configuration = hostContext.Configuration;

            var configurations = configuration.GetSection("KafkaConfigurations").Get<KafkaConfigurations>();
            if (configurations is null)
                throw new ArgumentNullException("KafkaConfigurations cannot be null");

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
                        o.AgentHost = "jaeger";
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

            // Note: Switch between Zipkin/Jaeger/OTLP/Console by setting UseTracingExporter in appsettings.json.
            var tracingExporter = configuration.GetValue<string>("UseTracingExporter").ToLowerInvariant();

            // Note: Switch between Prometheus/OTLP/Console by setting UseMetricsExporter in appsettings.json.
            var metricsExporter = configuration.GetValue<string>("UseMetricsExporter").ToLowerInvariant();

            // Note: Switch between Console/OTLP by setting UseLogExporter in appsettings.json.
            var logExporter = configuration.GetValue<string>("UseLogExporter").ToLowerInvariant();

            // Note: Switch between Explicit/Exponential by setting HistogramAggregation in appsettings.json
            var histogramAggregation = configuration.GetValue<string>("HistogramAggregation").ToLowerInvariant();

            // Build a resource configuration action to set service information.
            Action<ResourceBuilder> configureResource = r => r.AddService(
                serviceName: configuration.GetValue<string>("ServiceName"),
                serviceVersion: typeof(Program).Assembly.GetName().Version?.ToString() ?? "unknown",
                serviceInstanceId: Environment.MachineName);

            services.AddOpenTelemetry()
                .WithTracing(tracingProviderBuilder =>
                {
                    tracingProviderBuilder
                        .AddSource(serviceName)
                        .SetResourceBuilder(ResourceBuilder.CreateDefault()
                            .AddService(serviceName: serviceName, serviceVersion: serviceVersion))
                        .AddHttpClientInstrumentation()
                        .AddAspNetCoreInstrumentation();

                    switch (tracingExporter)
                    {
                        case "jaeger":
                            tracingProviderBuilder.AddJaegerExporter();

                            tracingProviderBuilder.ConfigureServices(services =>
                            {
                                // Use IConfiguration binding for Jaeger exporter options.
                                services.Configure<JaegerExporterOptions>(configuration.GetSection("Jaeger"));

                                // Customize the HttpClient that will be used when JaegerExporter is configured for HTTP transport.
                                services.AddHttpClient("JaegerExporter", configureClient: (client) => client.DefaultRequestHeaders.Add("X-MyCustomHeader", "value"));
                            });
                            break;

                        case "zipkin":
                            tracingProviderBuilder.AddZipkinExporter();

                            tracingProviderBuilder.ConfigureServices(services =>
                            {
                                // Use IConfiguration binding for Zipkin exporter options.
                                services.Configure<ZipkinExporterOptions>(configuration.GetSection("Zipkin"));
                            });
                            break;

                        case "otlp":
                            tracingProviderBuilder.AddOtlpExporter(otlpOptions =>
                            {
                                // Use IConfiguration directly for Otlp exporter endpoint option.
                                otlpOptions.Endpoint = new Uri(configuration.GetValue<string>("Otlp:Endpoint"));
                            });
                            break;

                        default:
                            tracingProviderBuilder.AddConsoleExporter();
                            break;
                    }

                    //tracingProviderBuilder.Build();
                })
                .WithMetrics(metricsProviderBuilder =>
                {
                    metricsProviderBuilder
                        .SetResourceBuilder(ResourceBuilder.CreateDefault()
                            .AddService(serviceName: serviceName, serviceVersion: serviceVersion))
                        //.AddRuntimeInstrumentation()
                        .AddHttpClientInstrumentation()
                        .AddAspNetCoreInstrumentation();

                    switch (histogramAggregation)
                    {
                        case "exponential":
                            metricsProviderBuilder.AddView(instrument =>
                            {
                                return instrument.GetType().GetGenericTypeDefinition() == typeof(Histogram<>)
                                    //? new Base2ExponentialBucketHistogramConfiguration()
                                    ? new ExplicitBucketHistogramConfiguration { Boundaries = new double[] { 1, 2, 5, 10 } }
                                    : null;
                            });
                            break;
                        default:
                            // Explicit bounds histogram is the default.
                            // No additional configuration necessary.
                            break;
                    }

                    switch (metricsExporter)
                    {
                        case "prometheus":
                            metricsProviderBuilder.AddPrometheusExporter();
                            break;
                        case "otlp":
                            metricsProviderBuilder.AddOtlpExporter(otlpOptions =>
                            {
                                // Use IConfiguration directly for Otlp exporter endpoint option.
                                otlpOptions.Endpoint = new Uri(configuration.GetValue<string>("Otlp:Endpoint"));
                            });
                            break;
                        default:
                            metricsProviderBuilder.AddConsoleExporter();
                            break;
                    }

                    //metricsProviderBuilder.Build();
                });

            //// Clear default logging providers used by WebApplication host.
            //builder.Logging.ClearProviders();

            //// Configure OpenTelemetry Logging.
            //builder.Logging.AddOpenTelemetry(options =>
            //{
            //    // Note: See appsettings.json Logging:OpenTelemetry section for configuration.

            //    var resourceBuilder = ResourceBuilder.CreateDefault();
            //    configureResource(resourceBuilder);
            //    options.SetResourceBuilder(resourceBuilder);

            //    switch (logExporter)
            //    {
            //        case "otlp":
            //            options.AddOtlpExporter(otlpOptions =>
            //            {
            //                // Use IConfiguration directly for Otlp exporter endpoint option.
            //                otlpOptions.Endpoint = new Uri(builder.Configuration.GetValue<string>("Otlp:Endpoint"));
            //            });
            //            break;
            //        default:
            //            options.AddConsoleExporter();
            //            break;
            //    }
            //});

            services.AddSingleton(sp => new ActivitySource(serviceName, serviceVersion));
        }
    }
}
