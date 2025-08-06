using Business.Models;
using Core.MessageBus;

namespace Business.Services
{
    public class ExecuteAnythingService
    {
        private readonly IProducerService _producerServices;

        public ExecuteAnythingService(IProducerService producerServices)
        {
            _producerServices = producerServices;
        }

        public async Task ExecuteAnything(Key key, Value value)
        {
            if (value?.Input == "erro")
                throw new Exception("ExecuteAnything - Testando exception");

            Console.WriteLine($"key: {key?.Id} - value: {value?.Input}");

            if (value?.Input is not null && value.Input.Equals("producer", StringComparison.InvariantCultureIgnoreCase))
                await _producerServices.ProducerAsync<string, string>("message-topic", $"key: {key?.Id}", $"value: {value?.Input}", false, false);
        }

        public Task ExecuteAnything(Value value)
        {
            if (value?.Input == "erro")
                throw new Exception("ExecuteAnything - Testando exception");            

            Console.WriteLine($"value: {value?.Input}");

            return Task.CompletedTask;
        }

        public Task ExecuteAnything(string key, string value)
        {
            if (value == "erro")
                throw new Exception("ExecuteAnything - Testando exception");

            Console.WriteLine($"key: {key} - value: {value}");

            return Task.CompletedTask;
        }
    }
}
