using System;
using System.Threading;
using Confluent.Kafka;

namespace KafkaElasticConsumer
{
    public class KafkaConsumer
    {
        private readonly string bootstrapServer;
        private readonly string groupId;

        public KafkaConsumer(String bootstrapServer, String groupId) {
            this.bootstrapServer = bootstrapServer;
            this.groupId = groupId;
        }

        public void Run() {
            ConsumerConfig configs = new ConsumerConfig {
                GroupId = groupId,
                BootstrapServers = bootstrapServer,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            using(var c = new ConsumerBuilder<String, String>(configs).Build()) {
                c.Subscribe("twitter");
                CancellationTokenSource cancellationToken = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true;
                    cancellationToken.Cancel();
                };
                try
                {
                    while (true)
                    {
                        try
                        {
                            ConsumeResult<string, string> result = c.Consume(cancellationToken.Token);
                            Console.WriteLine($"Consumed message '{result.Message.Value}' at: '{result.TopicPartitionOffset}'.");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error ocurried: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    c.Close();
                }
            }
        }
    }
}
