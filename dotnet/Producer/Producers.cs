using Confluent.Kafka;

namespace DotnetProducer;

public class Producers
{
    private readonly string _topic = "TP-TRANSACTIONS";

    public void BasicProducer(long events, string key)
    {
        Console.WriteLine("Starting basicProducer...");

        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092",
            Acks = Acks.All,            
        };

        DateTime startTime = DateTime.Now;

        using (var producer = new ProducerBuilder<string, string>(config).Build())
        {
            try
            {
                for (int i = 0; i < events; i++)
                {
                    var value = $"Message {i}";

                    var message = new Message<string, string> 
                    {
                        Key = key,
                        Value = value,
                    };

                    producer.Produce(_topic, message);
                };
            }
            catch (ProduceException<string, string> e)
            {
                Console.WriteLine($"Delivery failed: {e.Error.Reason}");
            }

            producer.Flush();
        }

        var elapsedTimeInMs = (DateTime.Now - startTime).TotalMilliseconds;
        Console.WriteLine($"Elapsed time: {elapsedTimeInMs} ms");
    }
}