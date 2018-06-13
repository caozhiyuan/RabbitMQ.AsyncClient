using System;
using System.Text;
using System.Threading;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using RabbitMQ.Client;

namespace RabbitMQ.AsyncClient.WebSyncTest
{
    public class Startup
    {
        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {
            app.Run(async (context) =>
            {
                var messageBodyBytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new Test
                {
                    Id = Interlocked.Increment(ref id),
                    Name = "Queue asynctest in virtual host /Queue asynctest in virtual host /Queue asynctest in virtual host /Queue asynctest in virtual host /"
                }));
                LazyModel.Value.BasicPublish("asynctest", "asynctest", true, null, messageBodyBytes);

                await context.Response.WriteAsync("Hello World!");
            });
        }

        private class Test
        {
            public int Id { get; set; }

            public string Name { get; set; }
        }

        private int id = 0;
        private static readonly Lazy<IModel> LazyModel = new Lazy<IModel>(GetMqChannel);

        private static IModel GetMqChannel()
        {
            var factory = new ConnectionFactory
            {
                UserName = "shampoo",
                Password = "123456",
                VirtualHost = "/",
                HostName = "10.1.62.66",
                DispatchConsumersAsync = true
            };

            var conn = factory.CreateConnection();

            IModel channel = conn.CreateModel();
            channel.ExchangeDeclare("asynctest", "topic");
            channel.QueueDeclare("asynctest", true, false, false, null);
            channel.QueueBind("asynctest", "asynctest", "asynctest", null);
            return channel;
        }
    }
}
