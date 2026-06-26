using ProdAndConsKafka.Application.DTOs;
using ProdAndConsKafka.Application.Interfaces;
using ProdAndConsKafka.Domain.Entities;

namespace ProdAndConsKafka.Application.Handlers;

public class PublishMessageHandler
{
    private readonly IKafkaProducer _kafkaProducer;

    public PublishMessageHandler(IKafkaProducer kafkaProducer)
    {
        _kafkaProducer = kafkaProducer;
    }

    public async Task<PublishMessageResponse> HandleAsync(
        PublishMessageRequest request,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(request.Content))
        {
            throw new ArgumentException("Content is required.", nameof(request));
        }

        var message = new Message
        {
            Id = Guid.NewGuid(),
            Content = request.Content.Trim(),
            CreatedAt = DateTime.UtcNow
        };

        await _kafkaProducer.ProduceAsync(message, cancellationToken);

        return new PublishMessageResponse
        {
            MessageId = message.Id,
            Success = true
        };
    }
}
