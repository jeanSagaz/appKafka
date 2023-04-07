using Consumer.Worker.Extensions;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(ProgramExtensions.Configure)
    .Build();

await host.RunAsync();
