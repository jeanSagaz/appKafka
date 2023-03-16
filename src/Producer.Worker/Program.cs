using Producer.Worker.Configurations;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(ConfigurationServices.Configure)
    .Build();

await host.RunAsync();
