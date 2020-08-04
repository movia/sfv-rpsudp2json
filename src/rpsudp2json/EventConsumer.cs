using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace RpsUdpToJson
{
    class EventConsumer<T> : DefaultBasicConsumer
    {
        private readonly Action<T> action;
        private readonly ILogger logger;
        private readonly string eventTypeName;

        public EventConsumer(Action<T> action, string eventType = null, ILogger logger = null)
        {
            this.action = action;
            this.logger = logger;
            eventTypeName = eventType ?? LowerCamelCase(typeof(T).Name);
        }

        private string LowerCamelCase(string s) => Char.ToLowerInvariant(s[0]) + s.Substring(1);

        public override void HandleBasicDeliver(string consumerTag, ulong deliveryTag, bool redelivered, string exchange, string routingKey, IBasicProperties properties, byte[] body)
        {
            if (properties.Headers.TryGetValue("event_type", out object eventType) &&
                string.Equals(Encoding.UTF8.GetString(eventType as byte[]), eventTypeName, StringComparison.OrdinalIgnoreCase))
            {
                try
                {
                    var eventObject = JsonSerializer.Deserialize<T>(body);

                    if (eventObject == null)
                    {
                        logger.LogWarning($"Event data was unexpectedly null");
                    }
                    else
                    {
                        action(eventObject);
                    }
                }
                catch (JsonException ex)
                {
                    logger.LogWarning(ex, $"Failed to parse {eventTypeName}: {Encoding.UTF8.GetString(body)}");
                }
            }
        }
    }
}
