using Business.Models;
using Core.MessageBus;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.Extensions.DependencyInjection;
using Producer.AppConsole.Configurations;

IProducerService? _producerService;

InitConfiguration();
Execute();

void Execute()
{
    var input = string.Empty;

    do
    {
        Console.WriteLine("digite o topico(1, 2): ");
        var topicId = Console.ReadLine();

        Console.WriteLine("digite um valor: ");
        input = Console.ReadLine();

        var value = new Value()
        {
            Input = input,
        };

        var key = new Key()
        {
            Id = Guid.NewGuid().ToString(),
        };

        var topic = topicId switch
        {
            "1" => "authorizer-topic",
            "2" => "payment-topic",
            _ => "authorizer-topic"
        };

        Console.WriteLine($"tópico: {topic} - key id: {key.Id} - value: {value.Input}");

        _producerService?.ProducerAsync<Key, Value>(topic, key, value).Wait();
    } while (!string.IsNullOrEmpty(input) && input[0] != (char)27);

    //_producerService?.ProducerAsync<string, string>("message-topic", "key", "value", false, false).Wait();

    Console.WriteLine("fim...");
    Console.ReadKey();
}

void InitConfiguration()
{
    var serviceProvider = new ServiceCollection();
    serviceProvider.ConfigureServices();
    var services = serviceProvider.BuildServiceProvider();

    _producerService = services.GetService<IProducerService>() ?? throw new ArgumentException("Ocorreu um erro ao iniciar 'IProducerService'");
}