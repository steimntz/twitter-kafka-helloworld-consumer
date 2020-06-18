using System;
using KafkaElasticConsumer;

namespace KafkaElasticConsumer.Main
{
    class Program
    {
        static void Main(string[] args)
        {
            KafkaConsumer kafkaConsumer = new KafkaConsumer("127.0.0.1:9092", "first-csharp-app");
            kafkaConsumer.Run();
        }
    }
}
