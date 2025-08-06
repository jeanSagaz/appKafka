using Adapters.Models;
using Adapters.Producer;
using Core.MessageBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace Producer.AppConsole.Configurations
{
    public static class DependencyInjectionConfiguration
    {
        public static void ConfigureServices(this IServiceCollection services)
        {
            var currentDirectory = string.Empty;
#if DEBUG
            currentDirectory = Directory.GetCurrentDirectory();
#else
    currentDirectory = AppDomain.CurrentDomain.BaseDirectory;
#endif

            //string environmentName = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
            string environmentName = "Development";
            var builder = new ConfigurationBuilder()
              .SetBasePath(currentDirectory)
              .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
              .AddJsonFile($"appsettings.{environmentName}.json", optional: true);

            IConfiguration configuration = builder.Build();

            var appSettings = new AppSettings();
            configuration.Bind(appSettings);
            services.AddSingleton(appSettings);

            if (appSettings?.KafkaConfigurations is null)
                throw new ArgumentNullException("Kafka configurations cannot be null");

            services.AddSingleton(new ActivitySource("Producer.AppConsole"));
            services.AddLogging(loggingBuilder => loggingBuilder.AddConsole());
            services.AddTransient<IProducerService, ProducerService>();
        }
    }
}
