using ProdAndConsKafka.Domain.Entities;

namespace ProdAndConsKafka.Application.Interfaces;

public interface IMessageHandler
{
    Task HandleAsync(Message message, CancellationToken cancellationToken = default);
}
