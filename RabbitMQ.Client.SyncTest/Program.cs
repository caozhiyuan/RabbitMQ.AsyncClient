using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
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


        public class Test
        {
            public int Id { get; set; }

            public string Name { get; set; }
        }

        private static void AsyncTest()
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

            var consumconn = factory.CreateConnection();
            try
            {
                IModel channel = conn.CreateModel();
                channel.ExchangeDeclare("asynctest", "topic");
                channel.QueueDeclare("asynctest", true, false, false, null);
                channel.QueueBind("asynctest", "asynctest", "asynctest", null);
                channel.FlowControl += (s, e) =>
                {
                    Console.WriteLine("FlowControl");
                };


                IModel consumchannel = consumconn.CreateModel();
                consumchannel.BasicQos(0, 100, false);
                AsyncEventingBasicConsumer consumer = new AsyncEventingBasicConsumer(consumchannel);

                int mm = 0;
                Stopwatch sw0 = new Stopwatch();
                sw0.Start();
                consumer.Received += (ch, ea) =>
                {
                    Interlocked.Increment(ref mm);
                    if (mm % 10000 == 0)
                    {
                        sw0.Stop();
                        Console.WriteLine($" {mm}recv {sw0.ElapsedMilliseconds}ms {Encoding.UTF8.GetString(ea.Body)}");
                        sw0.Restart();
                    }
                    consumchannel.BasicAck(ea.DeliveryTag, false);
                    return Task.CompletedTask;
                };
                var consumerTag = consumchannel.BasicConsume("asynctest", false, consumer);
                Console.WriteLine($"consumerTag {consumerTag}");

                int id = 0;
                while (true)
                {
                    Stopwatch sw = new Stopwatch();
                    sw.Start();

                    const int c = 10000;
                    CountdownEvent k = new CountdownEvent(c);
                    Parallel.For(0, c, (i) =>
                    {
                        var messageBodyBytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new Test()
                        {
                            Id = Interlocked.Increment(ref id),
                            Name = "Queue asynctest in virtual host /Queue asynctest in virtual host /Queue asynctest in virtual host /Queue asynctest in virtual host /Queue asynctest in virtual host /Queue asynctest in virtual host /Queue asynctest in virtual host /Queue asynctest in virtual host /Queue asynctest in virtual host /Queue asynctest in virtual host /Queue asynctest in virtual host /Queue asynctest in virtual host Queue asynctest in virtual host /Queue asynctest in virtual host /Queue asynctest in virtual host /Queue asynctest in virtual host /Queue asynctest in virtual host /Queue asynctest in virtual host /Queue asynctest in virtual host /Queue asynctest in virtual host /Queue asynctest in virtual host /Queue asynctest in virtual host /Queue asynctest in virtual host /Queue asynctest in virtual host /"
                        }));
                        channel.BasicPublish("asynctest", "asynctest", true, null, messageBodyBytes);
                        k.Signal(1);
                    });
                    k.Wait();
                    Console.WriteLine("mqtest " + sw.ElapsedMilliseconds);
                }
            }
            finally
            {
                conn.Dispose();
            }
        }
    }
}
