using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace RpsUdpToJson
{
    class AsyncEventConsumer<T> : AsyncDefaultBasicConsumer
    {
        private readonly Func<T, Task> action;
        private readonly ILogger logger;
        private readonly string eventTypeName;
        private readonly JsonSerializerOptions jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        public AsyncEventConsumer(Func<T, Task> action, string? eventType = null, ILogger? logger = null)
        {
            this.action = action;
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            eventTypeName = eventType ?? LowerCamelCase(typeof(T).Name);
        }

        private string LowerCamelCase(string s) => char.ToLowerInvariant(s[0]) + s.Substring(1);

        public override async Task HandleBasicDeliver(string consumerTag, ulong deliveryTag, bool redelivered, string exchange, string routingKey, IBasicProperties properties, ReadOnlyMemory<byte> body)
        {
            if (properties.Headers.TryGetValue("event_type", out object? eventType) &&
                string.Equals(Encoding.UTF8.GetString((byte[])eventType), eventTypeName, StringComparison.OrdinalIgnoreCase))
            {
                try
                {
                    var eventObject = JsonSerializer.Deserialize<T>(body.Span, jsonOptions);

                    if (eventObject == null)
                    {
                        logger.LogWarning($"Event data was unexpectedly null");
                    }
                    else
                    {
                        await action(eventObject);
                    }
                }
                catch (JsonException ex)
                {
                    logger.LogWarning(ex, $"Failed to parse {eventTypeName}: {Encoding.UTF8.GetString(body.Span)}");
                }
            }
        }
    }
}
