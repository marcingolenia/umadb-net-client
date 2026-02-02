using Client;
using UmaDb.Core;

var umaClient = UmaClient.UmaClient.Connect("localhost", 5000);
    
var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

app.MapGet("/", () => "Hello World!");

app.Run();