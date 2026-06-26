using ProdAndConsKafka.Domain.Entities;

namespace ProdAndConsKafka.Application.Interfaces;

public interface IKafkaProducer
{
    Task ProduceAsync(Message message, CancellationToken cancellationToken = default);
}
