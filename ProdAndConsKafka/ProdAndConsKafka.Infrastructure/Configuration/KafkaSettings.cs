namespace ProdAndConsKafka.Infrastructure.Configuration;

public class KafkaSettings
{
    public const string SectionName = "Kafka";

    public bool Enabled { get; set; } = true;
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string Topic { get; set; } = "messages";
    public string ConsumerGroupId { get; set; } = "prod-cons-group";
    public string AutoOffsetReset { get; set; } = "Earliest";
    public int SessionTimeoutMs { get; set; } = 6000;
    public int ConsumerRetryDelaySeconds { get; set; } = 10;
}
