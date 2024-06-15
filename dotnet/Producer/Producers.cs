using Confluent.Kafka;

namespace DotnetProducer;

public class Producers
{
    public void BasicProducer(long events)
    {
        Console.WriteLine("Starting basicProducer...");

        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092",
            Acks = Acks.All,
        };

        DateTime startTime = DateTime.Now;

        var elapsedTimeInMs = (DateTime.Now - startTime).TotalMilliseconds;
        Console.WriteLine($"Elapsed time: {elapsedTimeInMs} ms");
    }
}