using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using Newtonsoft.Json;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;

using VehicleTracker.Contracts;

namespace RpsUdpToJson
{
    public class RpsUdpToJsonWorker : RetryWorker
    {
        private readonly Service serviceHost;
        private readonly ILogger<RpsUdpToJsonWorker> logger;
        private readonly IConfiguration config;
        private readonly UdpConverter udpConverter;

        private IConnection connection;
        private IModel channel;
        private EventingBasicConsumer consumer;

        public RpsUdpToJsonWorker(Service serviceHost, ILogger<RpsUdpToJsonWorker> logger, IConfiguration config, UdpConverter udpConverter)
            : base(serviceHost, logger, config, null)
        {
            this.serviceHost = serviceHost;
            this.logger = logger;
            this.config = config;
            this.udpConverter = udpConverter;
        }

        protected override async Task Work()
        {
            var url = config.GetSection("RabbitMQ")?.GetValue<string>("Url");
            logger.LogInformation($"Connecting to url: {url}");
            var factory = new ConnectionFactory() { Uri = new Uri(url) };
            connection = factory.CreateConnection();
            channel = connection.CreateModel();

            var lastMessageProcessed = DateTime.UtcNow;
            var udpExchange = "vehicle-position-udp-raw";
            var jsonExchange = "vehicle-position-json";
            var workQueue = "rpsudp2json-vehicle-position-udp-raw";

            var arguments = new Dictionary<string, object>();
            arguments.Add("x-message-ttl", 3 * 60 * 60 * 1000); // 3 hours
            channel.ExchangeDeclare(jsonExchange, "fanout", durable: true, autoDelete: false);
            channel.QueueDeclare(workQueue, durable: true, exclusive: false, autoDelete: false, arguments);
            channel.QueueBind(workQueue, udpExchange, string.Empty);

            consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var bytes = ea.Body;

                if (bytes == null || bytes.Length == 0)
                {
                    logger.LogTrace("Recived empty position message");
                    return;
                }

                switch (bytes[0])
                {
                    case 2:
                        if (udpConverter.TryParseVehiclePosition(bytes, out var vehiclePositon))
                        {                            
                            var vehiclePositonEvent = new VehiclePositionEvent() { VehiclePosition = vehiclePositon };
                            var routingKey = $"{vehiclePositonEvent.EventType}.{vehiclePositon.VehicleRef}";

                            channel.BasicPublish(jsonExchange, routingKey, body: Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(vehiclePositonEvent)));
                            //ResetWatchdog();
                        }
                        break;
                    case 255:
                        /* Time Message used for SLA - Ignore. */
                        break;
                    default:
                        logger.LogTrace($"Unknown message type: {bytes[0]}.");
                        break;
                }
            };

            logger.LogInformation($"Beginning to consume ...");

            channel.BasicConsume(queue: workQueue,
                                 autoAck: true,
                                 consumer: consumer);

            await serviceHost.CancellationToken.WaitHandle.WaitOneAsync(CancellationToken.None);
        }
    }
}
