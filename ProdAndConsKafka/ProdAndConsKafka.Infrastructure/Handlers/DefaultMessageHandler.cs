using Microsoft.Extensions.Logging;
using ProdAndConsKafka.Application.Interfaces;
using ProdAndConsKafka.Domain.Entities;

namespace ProdAndConsKafka.Infrastructure.Handlers;

public class DefaultMessageHandler : IMessageHandler
{
    private readonly ILogger<DefaultMessageHandler> _logger;

    public DefaultMessageHandler(ILogger<DefaultMessageHandler> logger)
    {
        _logger = logger;
    }

    public Task HandleAsync(Message message, CancellationToken cancellationToken = default)
    {
        Console.WriteLine("========================================");
        Console.WriteLine("  MENSAGEM RECEBIDA DO KAFKA");
        Console.WriteLine($"  Id:      {message.Id}");
        Console.WriteLine($"  Conteúdo: {message.Content}");
        Console.WriteLine($"  Data:    {message.CreatedAt:yyyy-MM-dd HH:mm:ss} UTC");
        Console.WriteLine("========================================");

        _logger.LogInformation(
            "Consumed message {MessageId} created at {CreatedAt}: {Content}",
            message.Id,
            message.CreatedAt,
            message.Content);

        return Task.CompletedTask;
    }
}
