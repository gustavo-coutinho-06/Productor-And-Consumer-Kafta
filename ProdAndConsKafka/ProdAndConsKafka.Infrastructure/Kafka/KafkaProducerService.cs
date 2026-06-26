using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ProdAndConsKafka.Application.Interfaces;
using ProdAndConsKafka.Domain.Entities;
using ProdAndConsKafka.Infrastructure.Configuration;

namespace ProdAndConsKafka.Infrastructure.Kafka;

public class KafkaProducerService : IKafkaProducer, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly KafkaSettings _settings;
    private readonly ILogger<KafkaProducerService> _logger;

    public KafkaProducerService(
        IOptions<KafkaSettings> settings,
        ILogger<KafkaProducerService> logger)
    {
        _settings = settings.Value;
        _logger = logger;

        var config = new ProducerConfig
        {
            BootstrapServers = _settings.BootstrapServers
        };

        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task ProduceAsync(Message message, CancellationToken cancellationToken = default)
    {
        var payload = JsonSerializer.Serialize(message);

        var result = await _producer.ProduceAsync(
            _settings.Topic,
            new Confluent.Kafka.Message<string, string>
            {
                Key = message.Id.ToString(),
                Value = payload
            },
            cancellationToken);

        _logger.LogInformation(
            "Message {MessageId} produced to {Topic} at offset {Offset}",
            message.Id,
            result.Topic,
            result.Offset);
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }
}
