namespace ProdAndConsKafka.Application.DTOs;

public class PublishMessageResponse
{
    public Guid MessageId { get; set; }
    public bool Success { get; set; }
}
