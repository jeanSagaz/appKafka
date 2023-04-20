using Adapters.Consumer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace Adapters.Extensions
{
    public static class WorkerExtensions
    {
        public static void AddAsyncTopicConsumer<TService, TKey, TValue>(this IServiceCollection services,
           string host,
           string topic,
           string groupId,
           Func<TService, TKey, TValue, Task> functionToExecute,           
           bool? enableDeserializer = false)
        {
            if (services is null) throw new ArgumentNullException(nameof(services));
            if (string.IsNullOrEmpty(host)) throw new ArgumentException($"'{nameof(host)}' cannot be null or empty.", nameof(host));
            if (string.IsNullOrEmpty(topic)) throw new ArgumentException($"'{nameof(host)}' cannot be null or empty.", nameof(topic));
            if (functionToExecute is null) throw new ArgumentNullException(nameof(functionToExecute));

            services.AddSingleton<IHostedService>(sp =>
            //services.AddHostedService(sp =>
                    new AsyncTopicServiceWorker<TKey, TValue>(
                        sp.GetService<ILogger<AsyncTopicServiceWorker<TKey, TValue>>>(),
                        host,
                        topic,
                        groupId,
                        sp.GetRequiredService<ActivitySource>(),
                        async (key, value) => await functionToExecute(sp.GetService<TService>(), key, value),
                        enableDeserializer
                    )
                );
        }

        public static void AddAsyncTopicConsumer<TService, TValue>(this IServiceCollection services,
            string host,
            string topic,
            string groupId,
            Func<TService, TValue, Task> functionToExecute,
            bool? enableDeserializer = false)
        {
            if (services is null) throw new ArgumentNullException(nameof(services));
            if (string.IsNullOrEmpty(host)) throw new ArgumentException($"'{nameof(host)}' cannot be null or empty.", nameof(host));
            if (string.IsNullOrEmpty(topic)) throw new ArgumentException($"'{nameof(topic)}' cannot be null or empty.", nameof(topic));
            if (functionToExecute is null) throw new ArgumentNullException(nameof(functionToExecute));

            services.AddSingleton<IHostedService>(sp =>
            //services.AddHostedService(sp =>
                    new AsyncTopicServiceWorker<object?, TValue>(
                        sp.GetService<ILogger<AsyncTopicServiceWorker<object?, TValue>>>(),
                        host,
                        topic,
                        groupId,
                        sp.GetRequiredService<ActivitySource>(),
                        async (value) => await functionToExecute(sp.GetService<TService>(), value),
                        enableDeserializer
                    )
                );
        }

        public static void AddAsyncTopicConsumer<TService, TKey, TValue>(this IServiceCollection services,
           string host,
           string topic,
           string groupId,
           Action<TService, TKey, TValue> actionToExecute,
           bool? enableDeserializer = false)
        {
            if (services is null) throw new ArgumentNullException(nameof(services));
            if (string.IsNullOrEmpty(host)) throw new ArgumentException($"'{nameof(host)}' cannot be null or empty.", nameof(host));
            if (string.IsNullOrEmpty(topic)) throw new ArgumentException($"'{nameof(topic)}' cannot be null or empty.", nameof(topic));
            if (actionToExecute is null) throw new ArgumentNullException(nameof(actionToExecute));

            services.AddSingleton<IHostedService>(sp =>
            //services.AddHostedService(sp =>
                    new AsyncTopicServiceWorker<TKey, TValue>(
                        sp.GetService<ILogger<AsyncTopicServiceWorker<TKey, TValue>>>(),
                        host,
                        topic,
                        groupId,
                        sp.GetRequiredService<ActivitySource>(),
                        (key, value) => actionToExecute(sp.GetService<TService>(), key, value),
                        enableDeserializer
                    )
                );
        }
    }
}
