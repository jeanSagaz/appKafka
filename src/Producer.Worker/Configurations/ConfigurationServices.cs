namespace Producer.Worker.Configurations
{
    public static class ConfigurationServices
    {
        public static void Configure(HostBuilderContext hostContext, IServiceCollection services)
        {
            services.AddHostedService<ProducerWorker>();
        }
    }
}
