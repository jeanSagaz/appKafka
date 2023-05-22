using OpenTelemetry.Exporter;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Consumer.Worker.Extensions
{
    public static class OpenTelemetryExtensions
    {
        public static void AddOpenTelemetry(this IServiceCollection services, IConfiguration configuration)
        {
            string serviceName = configuration.GetValue<string>("ServiceName").ToLowerInvariant();
            string serviceVersion = "1.0.0";

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
                serviceName: serviceName,
                serviceVersion: typeof(Program).Assembly.GetName().Version?.ToString() ?? "unknown",
                serviceInstanceId: Environment.MachineName);

            services.AddOpenTelemetry()
                .WithTracing(tracingProviderBuilder =>
                {
                    tracingProviderBuilder
                        .AddSource(serviceName)
                        .SetResourceBuilder(ResourceBuilder.CreateDefault()
                            .AddService(serviceName: serviceName, serviceVersion: serviceVersion))
                        .AddHttpClientInstrumentation(p =>
                        {
                            p.RecordException = true;
                        })
                        .AddAspNetCoreInstrumentation(p =>
                        {
                            p.RecordException = true;
                        });

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
                });

            // Configure OpenTelemetry Logging.
            services.AddLogging(build =>
            {
                build.AddOpenTelemetry(options =>
                {
                    // Note: See appsettings.json Logging:OpenTelemetry section for configuration.
                    var resourceBuilder = ResourceBuilder.CreateDefault();
                    configureResource(resourceBuilder);
                    options
                        .SetResourceBuilder(resourceBuilder)
                        .AddProcessor(new ActivityEventExtensions()).IncludeScopes = true;

                    switch (logExporter)
                    {
                        case "otlp":
                            options.AddOtlpExporter(otlpOptions =>
                            {
                                // Use IConfiguration directly for Otlp exporter endpoint option.
                                otlpOptions.Endpoint = new Uri(configuration.GetValue<string>("Otlp:Endpoint"));
                            });
                            break;
                        default:
                            options.AddConsoleExporter();
                            break;
                    }
                });
            });

            services.Configure<OpenTelemetryLoggerOptions>(options =>
            {
                options.IncludeScopes = true;
                options.ParseStateValues = true;
                options.IncludeFormattedMessage = true;
            });

            services.AddSingleton(sp => new ActivitySource(serviceName, serviceVersion));
        }
    }
}
