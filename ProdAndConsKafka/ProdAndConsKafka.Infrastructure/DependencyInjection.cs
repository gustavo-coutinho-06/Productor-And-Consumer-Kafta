using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using ProdAndConsKafka.Application.Interfaces;
using ProdAndConsKafka.Infrastructure.Configuration;
using ProdAndConsKafka.Infrastructure.Handlers;
using ProdAndConsKafka.Infrastructure.Kafka;

namespace ProdAndConsKafka.Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddInfrastructure(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.Configure<KafkaSettings>(configuration.GetSection(KafkaSettings.SectionName));

        var kafkaSettings = configuration.GetSection(KafkaSettings.SectionName).Get<KafkaSettings>() ?? new KafkaSettings();

        services.AddSingleton<IKafkaProducer, KafkaProducerService>();
        services.AddScoped<IMessageHandler, DefaultMessageHandler>();

        if (kafkaSettings.Enabled)
        {
            services.AddHostedService<KafkaConsumerHostedService>();
        }

        return services;
    }
}
