using DotnetProducer;

var producer = new Producers();
// producer.BasicProducer(1000, "one");
producer.SyncronousProducer(1000, "two");