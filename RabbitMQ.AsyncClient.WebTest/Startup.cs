using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using RabbitMQ.Client;

namespace RabbitMQ.AsyncClient.WebTest
{
    public class Startup
    {
        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {
            app.Run(async (context) =>
            {
                var channel = await GetMqChannelAsync();
                var messageBodyBytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new Test
                {
                    Id = Interlocked.Increment(ref id),
                    Name = "Queue asynctest in virtual host /Queue asynctest in virtual host /Queue asynctest in virtual host /Queue asynctest in virtual host /"
                }));
                await channel.BasicPublish("asynctest", "asynctest", true, null, messageBodyBytes);

                await context.Response.WriteAsync("Hello World!");
            });
        }

        private class Test
        {
            public int Id { get; set; }

            public string Name { get; set; }
        }

        private int id;
        private IModel m_channel;
        private readonly SemaphoreSlim semaphoreSlim = new SemaphoreSlim(1, 1);

        private async Task<IModel> GetMqChannelAsync()
        {
            if (m_channel != null)
            {
                return m_channel;
            }

            await semaphoreSlim.WaitAsync();
            try
            {
                if (m_channel != null)
                {
                    return m_channel;
                }

                var factory = new ConnectionFactory
                {
                    UserName = "shampoo",
                    Password = "123456",
                    VirtualHost = "/",
                    HostName = "10.1.62.66",
                    DispatchConsumersAsync = true
                };

                var conn = await factory.CreateConnection();

                IModel channel = await conn.CreateModel();
                await channel.ExchangeDeclare("asynctest", "topic");
                await channel.QueueDeclare("asynctest", true, false, false, null);
                await channel.QueueBind("asynctest", "asynctest", "asynctest", null);

                m_channel = channel;

                return channel;
            }
            finally
            {
                semaphoreSlim.Release();
            }
        }
    }
}
