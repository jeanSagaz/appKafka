namespace Core.MessageBus
{
    public interface IProducerService
    {
        Task ProducerAsync<TKey, TValue>(string topic, TKey? key, TValue value, bool enableKeySerializer = false, bool enableValueSerializer = false);

        Task ProducerAsync<TValue>(string topic, TValue value, bool enableKeySerializer = false, bool enableValueSerializer = false);
    }
}
