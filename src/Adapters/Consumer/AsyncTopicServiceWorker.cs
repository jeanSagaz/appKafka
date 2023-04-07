using Adapters.Consumer.Enums;
using Adapters.Extensions;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

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
            ActivitySource activitySource,
            Func<TValue, Task>? dispatchFunc,            
            bool? enableDeserializer = false)
        : base(logger, host, topic, groupId, activitySource, enableDeserializer)
        {
            this.dispatchFunc = dispatchFunc;
        }

        public AsyncTopicServiceWorker(ILogger? logger,
            string host,
            string topic,
            string groupId,
            ActivitySource activitySource,
            Func<TValue, Task<bool>>? functionToRun,            
            bool? enableDeserializer = false)
        : base(logger, host, topic, groupId, activitySource, enableDeserializer)
        {
            this.functionToRun = functionToRun;
        }

        public AsyncTopicServiceWorker(ILogger? logger,
            string host,
            string topic,
            string groupId,
            ActivitySource activitySource,
            Action<TKey, TValue>? disposeAction,
            bool? enableDeserializer = false)
        : base(logger, host, topic, groupId, activitySource, enableDeserializer)
        {
            this.disposeAction = disposeAction;
        }

        protected override async Task<PostConsumeAction> Dispatch(Activity? receiveActivity, TKey key, TValue value)
        {
            using Activity dispatchActivity = this._activitySource.SafeStartActivity("AsyncTopicServiceWorker.Dispatch", ActivityKind.Internal, receiveActivity.Context);

            if (dispatchFunc is not null)
                await dispatchFunc(value);

            if (functionToRun is not null)
            {
                if (!await functionToRun(value))
                {
                    _logger.LogWarning("Continue in Kafka");
                    dispatchActivity?.SetEndTime(DateTime.UtcNow);

                    return PostConsumeAction.Reject;
                }
            }

            if (disposeAction is not null)
                disposeAction(key, value);

            dispatchActivity?.SetEndTime(DateTime.UtcNow);

            return PostConsumeAction.Commit;
        }
    }
}
