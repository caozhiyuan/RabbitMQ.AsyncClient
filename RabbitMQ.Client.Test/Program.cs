﻿using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
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

        public class Test
        {
            public int Id { get; set; }

            public string Name { get; set; }
        }

        private static async Task AsyncTest()
        {
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

                int mm = 0;
                Stopwatch sw0 = new Stopwatch();
                sw0.Start();
                consumer.Received += async (ch, ea) =>
                {
                    Interlocked.Increment(ref mm);
                    if (mm % 10000 == 0)
                    {
                        sw0.Stop();
                        Console.Write($" {mm}recv {sw0.ElapsedMilliseconds}ms {Encoding.UTF8.GetString(ea.Body)}");
                        sw0.Restart();
                    }
                    await channel.BasicAck(ea.DeliveryTag, false);
                };
                var consumerTag = await channel.BasicConsume("asynctest", false, consumer);
                Console.WriteLine($"consumerTag {consumerTag}");

                channel.FlowControl += (s, e) =>
                {
                    Console.WriteLine("FlowControl");
                    return Task.CompletedTask;
                };
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
                            Name = "Queue asynctest in virtual host /"
                        }));
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
