using Business.Models;
using Core.MessageBus;

namespace Business.Services
{
    public class ExecuteAnythingService
    {
        private readonly IProducerServices _producerServices;

        public ExecuteAnythingService(IProducerServices producerServices)
        {
            _producerServices = producerServices;
        }

        public async Task ThrowException(ExecuteAnythingRequest model)
        {            
            throw new Exception("ThrowException - Testando exception");
        }

        public async Task ExecuteAnything(ExecuteAnythingRequest model)
        {
            if (model?.Name == "Jean")
                throw new Exception("ExecuteAnything - Testando exception");            

            await _producerServices.ProducerAsync<string, string>("payment-topic", $"key: {model?.Name}", $"value: {model?.Name}");
        }

        public Task ExecuteAnything(string key, string value)
        {
            if (value == "Jean")
                throw new Exception();

            Console.WriteLine($"{key} - {value}");

            return Task.CompletedTask;
        }
    }
}
