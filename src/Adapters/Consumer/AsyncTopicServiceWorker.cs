using Adapters.Consumer.Enums;
using Microsoft.Extensions.Logging;

namespace Adapters.Consumer
{
    public class AsyncTopicServiceWorker<TKey, TValue> : TopicServiceWorker<TKey, TValue>
    {
        protected readonly Func<TValue, Task>? dispatchFunc;
        protected readonly Func<TValue, Task<bool>>? functionToRun;
        protected readonly Action<TKey, TValue>? disposeAction;

        public AsyncTopicServiceWorker(ILogger? logger,
            string host,
            string topic,
            string groupId,
            Func<TValue, Task>? dispatchFunc,            
            bool? enableDeserializer = false)
        : base(logger, host, topic, groupId, enableDeserializer)
        {
            this.dispatchFunc = dispatchFunc;
        }

        public AsyncTopicServiceWorker(ILogger? logger,
            string host,
            string topic,
            string groupId,
            Func<TValue, Task<bool>>? functionToRun,            
            bool? enableDeserializer = false)
        : base(logger, host, topic, groupId, enableDeserializer)
        {
            this.functionToRun = functionToRun;
        }

        public AsyncTopicServiceWorker(ILogger? logger,
            string host,
            string topic,
            string groupId,
            Action<TKey, TValue>? disposeAction,
            bool? enableDeserializer = false)
        : base(logger, host, topic, groupId, enableDeserializer)
        {
            this.disposeAction = disposeAction;
        }

        protected override async Task<PostConsumeAction> Dispatch(TKey key, TValue value)
        {
            if (dispatchFunc is not null)
                await dispatchFunc(value);

            if (functionToRun is not null)
            {
                if (!await functionToRun(value))
                {
                    _logger.LogWarning("Continue in Kafka");
                    return PostConsumeAction.Reject;
                }
            }

            if (disposeAction is not null)
                disposeAction(key, value);

            return PostConsumeAction.Commit;
        }
    }
}
