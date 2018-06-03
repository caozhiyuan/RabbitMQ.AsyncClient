using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Client.Test
{
    class Program
    {
        static void Main(string[] args)
        {
            AsyncTest().GetAwaiter().GetResult();
            Console.ReadLine();
        }

        private static async Task AsyncTest()
        {
            string msg =
                "{\"Id\":1,\"CityId\":1,\"CityFlag\":\"sz\",\"WebApiUrl\":\"https://api1.34580.com/\",\"ImageSiteUrl\":\"http://picpro-sz.34580.com/\",\"CityName\":\"苏州市\"}";

            var  factory = new ConnectionFactory
            {
                UserName = "shampoo",
                Password = "123456",
                VirtualHost = "/",
                HostName = "10.1.62.66",
                DispatchConsumersAsync = true
            };

            var conn = await factory.CreateConnection();
            try
            {
                IModel channel = await conn.CreateModel();
                await channel.ExchangeDeclare("asynctest", "topic");
                await channel.QueueDeclare("asynctest", true, false, false, null);
                await channel.QueueBind("asynctest", "asynctest", "asynctest", null);

                await channel.BasicQos(0, 100, false);
                AsyncEventingBasicConsumer consumer = new AsyncEventingBasicConsumer(channel);
                consumer.Received += async (ch, ea) =>
                {
                    Console.WriteLine(Encoding.UTF8.GetString(ea.Body));
                    await channel.BasicAck(ea.DeliveryTag, false);
                };
                var consumerTag = await channel.BasicConsume("asynctest", false, consumer);
                Console.ReadLine();

                var messageBodyBytes = Encoding.UTF8.GetBytes(msg);

                while (true)
                {
                    Stopwatch sw = new Stopwatch();
                    sw.Start();

                    const int c = 10000;
                    CountdownEvent k = new CountdownEvent(c);
                    Parallel.For(0, c, (i) =>
                    {
                        var task = channel.BasicPublish("asynctest", "asynctest", true, null, messageBodyBytes);
                        task.ContinueWith(n =>
                        {
                            if (n.IsFaulted)
                            {
                                Console.WriteLine($"{i} {n.Exception}");
                            }
                            k.Signal(1);
                        });
                    });
                    k.Wait();
                    Console.WriteLine("mqtest " + sw.ElapsedMilliseconds);
                    Console.ReadLine();
                }
            }
            finally
            {
                await conn.Dispose();
            }
        }
    }
}
