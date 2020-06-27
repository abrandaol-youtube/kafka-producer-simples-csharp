using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace kafka_producer_simples_csharp
{
    class Program
    {
        static async Task Main()
        {
            var config = new ProducerConfig {
                BootstrapServers = "omnibus-01.srvs.cloudkafka.com:9094",
                SaslUsername = "f5xaw18s",
                SaslPassword = "GDrH3mHRwasLltJ4O7JRde73sPUN0y6h",
                SaslMechanism = SaslMechanism.Plain,
                SecurityProtocol = SecurityProtocol.SaslPlaintext
            };

            using var p = new ProducerBuilder<Null, string>(config).Build();

            try
            {
                var count = 0;

                while (true)
                {
                    var dr = await p.ProduceAsync("f5xaw18s-apptest", new Message<Null, string> { Value = $"test: {count}" });
                    Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}' -> count: {count}");

                    Thread.Sleep(2000);
                }
            }
            catch (ProduceException<Null, string> e)
            {
                Console.WriteLine($"Delivery failed: {e.Error.Reason}");
            }
        }
    }
}
