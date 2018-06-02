using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Client.SyncTest
{
    class Program
    {
        static void Main(string[] args)
        {
            AsyncTest();
            Console.ReadLine();
        }

        private static void AsyncTest()
        {
            string msg =
                "{\"Id\":1,\"CityId\":1,\"CityFlag\":\"sz\",\"WebApiUrl\":\"https://api1.34580.com/\",\"ImageSiteUrl\":\"http://picpro-sz.34580.com/\",\"CityName\":\"苏州市\"}";

            var factory = new ConnectionFactory
            {
                UserName = "shampoo",
                Password = "123456",
                VirtualHost = "/",
                HostName = "10.1.62.66",
                DispatchConsumersAsync = true
            };

            var conn = factory.CreateConnection();
            try
            {
                IModel channel = conn.CreateModel();
                channel.ExchangeDeclare("asynctest", "topic");
                channel.QueueDeclare("asynctest", true, false, false, null);
                channel.QueueBind("asynctest", "asynctest", "asynctest", null);


                AsyncEventingBasicConsumer consumer = new AsyncEventingBasicConsumer(channel);
                consumer.Received += async (ch, ea) =>
                {
                    Console.WriteLine(Encoding.UTF8.GetString(ea.Body));
                    channel.BasicAck(ea.DeliveryTag, false);
                };
                var consumerTag = channel.BasicConsume("asynctest", false, consumer);


                var messageBodyBytes = Encoding.UTF8.GetBytes(msg);

                while (true)
                {
                    Stopwatch sw = new Stopwatch();
                    sw.Start();

                    const int c = 10000;
                    CountdownEvent k = new CountdownEvent(c);
                    Parallel.For(0, c, (i) =>
                    {
                        channel.BasicPublish("asynctest", "asynctest", true, null, messageBodyBytes);
                        k.Signal(1);
                    });
                    k.Wait();
                    Console.WriteLine("mqtest " + sw.ElapsedMilliseconds);
                    Console.ReadLine();
                }
            }
            finally
            {
                conn.Dispose();
            }
        }
    }
}
