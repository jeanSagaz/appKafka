using Consumer.Worker.Extensions;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(ProgamExtensions.Configure)
    .Build();

//app.UseHealthChecks("/hc", new HealthCheckOptions()
//{
//    Predicate = _ => true,
//    ResponseWriter = UIResponseWriter.WriteHealthCheckUIResponse
//});

//app.UseHealthChecksUI(options =>
//{
//    options.UIPath = "/hc-ui";
//    options.ApiPath = "/hc-ui-api";
//});

await host.RunAsync();
