using Microsoft.AspNetCore.Hosting;

namespace RabbitMQ.AsyncClient.WebSyncTest
{
    public class Program
    {
        public static void Main(string[] args)
        {
            new WebHostBuilder()
                .UseKestrel()
                .UseStartup<Startup>()
                .UseUrls("http://*:9008")
                .Build()
                .Run();
        }
    }
}
