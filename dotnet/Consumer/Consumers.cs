using Confluent.Kafka;

namespace DotnetConsumer;

public class Consumers
{
    private readonly string _topic = "TP-TRANSACTIONS";
    public void BasicConsumer()
    {
        Console.WriteLine("Starting basicConsumer...");

        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "GR-TRANSACTIONS-FOUR",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
            AutoCommitIntervalMs = 1000,
        };

        using (var consumer = new ConsumerBuilder<string, string>(config)
            .Build()
        )
        {
            consumer.Subscribe(_topic);

            Console.WriteLine("Polling messages from topic...");

            try
            {
                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(TimeSpan.FromSeconds(1));

                        if (consumeResult == null) continue;

                        Console.WriteLine($"Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}, Partition: {consumeResult.Partition}");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error ocurred: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                consumer.Close();
            }
        }
    }
}