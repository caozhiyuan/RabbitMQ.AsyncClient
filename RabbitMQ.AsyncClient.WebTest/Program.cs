using Microsoft.AspNetCore.Hosting;

namespace RabbitMQ.AsyncClient.WebTest
{
    public class Program
    {
        public static void Main(string[] args)
        {
            new WebHostBuilder()
                .UseKestrel()
                .UseStartup<Startup>()
                .UseUrls("http://*:9009")
                .Build()
                .Run();
        }
    }
}
