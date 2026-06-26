using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ProdAndConsKafka.Application.Interfaces;
using ProdAndConsKafka.Infrastructure.Configuration;

namespace ProdAndConsKafka.Infrastructure.Kafka;

public class KafkaConsumerHostedService : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly KafkaSettings _settings;
    private readonly ILogger<KafkaConsumerHostedService> _logger;

    public KafkaConsumerHostedService(
        IServiceScopeFactory scopeFactory,
        IOptions<KafkaSettings> settings,
        ILogger<KafkaConsumerHostedService> logger)
    {
        _scopeFactory = scopeFactory;
        _settings = settings.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunConsumerAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Kafka consumer failed. Retrying in {DelaySeconds}s. Ensure Kafka is running at {BootstrapServers}.",
                    _settings.ConsumerRetryDelaySeconds,
                    _settings.BootstrapServers);

                await Task.Delay(
                    TimeSpan.FromSeconds(_settings.ConsumerRetryDelaySeconds),
                    stoppingToken);
            }
        }
    }

    private async Task RunConsumerAsync(CancellationToken stoppingToken)
    {
        var autoOffsetReset = Enum.TryParse<AutoOffsetReset>(
            _settings.AutoOffsetReset,
            ignoreCase: true,
            out var parsedOffsetReset)
            ? parsedOffsetReset
            : AutoOffsetReset.Earliest;

        var config = new ConsumerConfig
        {
            BootstrapServers = _settings.BootstrapServers,
            GroupId = _settings.ConsumerGroupId,
            AutoOffsetReset = autoOffsetReset,
            SessionTimeoutMs = _settings.SessionTimeoutMs,
            EnableAutoCommit = true,
            SocketTimeoutMs = 5000,
            MetadataMaxAgeMs = 5000
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();

        try
        {
            consumer.Subscribe(_settings.Topic);

            _logger.LogInformation(
                "Kafka consumer connected on topic {Topic} with group {GroupId}",
                _settings.Topic,
                _settings.ConsumerGroupId);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = consumer.Consume(TimeSpan.FromSeconds(1));
                    if (consumeResult?.Message?.Value is null)
                    {
                        continue;
                    }

                    var message = JsonSerializer.Deserialize<Domain.Entities.Message>(consumeResult.Message.Value);
                    if (message is null)
                    {
                        _logger.LogWarning("Failed to deserialize message at offset {Offset}", consumeResult.Offset);
                        continue;
                    }

                    using var scope = _scopeFactory.CreateScope();
                    var handler = scope.ServiceProvider.GetRequiredService<IMessageHandler>();
                    await handler.HandleAsync(message, stoppingToken);
                }
                catch (ConsumeException ex) when (!stoppingToken.IsCancellationRequested)
                {
                    _logger.LogWarning(ex, "Kafka consume error");
                    throw;
                }
            }
        }
        finally
        {
            consumer.Close();
            _logger.LogInformation("Kafka consumer stopped");
        }
    }
}
