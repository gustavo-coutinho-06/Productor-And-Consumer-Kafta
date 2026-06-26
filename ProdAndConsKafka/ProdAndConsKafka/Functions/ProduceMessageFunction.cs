using System.Net;
using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using ProdAndConsKafka.Application.DTOs;
using ProdAndConsKafka.Application.Handlers;

namespace ProdAndConsKafka.Functions;

public class ProduceMessageFunction
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly PublishMessageHandler _handler;
    private readonly ILogger<ProduceMessageFunction> _logger;

    public ProduceMessageFunction(
        PublishMessageHandler handler,
        ILogger<ProduceMessageFunction> logger)
    {
        _handler = handler;
        _logger = logger;
    }

    [Function("ProduceMessage")]
    public async Task<IActionResult> Run(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = "messages")] HttpRequest req,
        CancellationToken cancellationToken)
    {
        PublishMessageRequest? request;

        try
        {
            request = await JsonSerializer.DeserializeAsync<PublishMessageRequest>(
                req.Body,
                JsonOptions,
                cancellationToken: cancellationToken);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Invalid JSON body");
            return new BadRequestObjectResult("Invalid JSON body.");
        }

        if (request is null)
        {
            return new BadRequestObjectResult("Request body is required.");
        }

        try
        {
            var response = await _handler.HandleAsync(request, cancellationToken);
            return new OkObjectResult(response);
        }
        catch (ArgumentException ex)
        {
            _logger.LogWarning(ex, "Validation failed");
            return new BadRequestObjectResult(ex.Message);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish message");
            return new ObjectResult("Failed to publish message.")
            {
                StatusCode = (int)HttpStatusCode.InternalServerError
            };
        }
    }
}
