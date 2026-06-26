using Microsoft.Extensions.DependencyInjection;
using ProdAndConsKafka.Application.Handlers;

namespace ProdAndConsKafka.Application;

public static class DependencyInjection
{
    public static IServiceCollection AddApplication(this IServiceCollection services)
    {
        services.AddScoped<PublishMessageHandler>();
        return services;
    }
}
