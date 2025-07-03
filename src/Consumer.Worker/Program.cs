//using Consumer.Worker.Extensions;

//IHost host = Host.CreateDefaultBuilder(args)
//    .ConfigureServices(ProgramExtensions.Configure)
//    .Build();

//await host.RunAsync();

using Consumer.Worker.Extensions;
using Microsoft.AspNetCore.Builder;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure(builder.Configuration);

var app = builder.Build();

//app.UseOpenTelemetry();

await app.RunAsync();
