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
        private readonly IVehicleJourneyAssignmentCache vehicleJourneyAssignmentCache;
        private readonly UdpConverter udpConverter;

        private ConnectionFactory? rabbitConnectionFactory;
        private IConnection? rabbitConnection = null;
        private IModel? rabbitChannel = null;
        private EventingBasicConsumer? consumer;

        private readonly string udpExchange = "vehicle-position-udp-raw";
        private readonly string jsonExchange = "vehicle-position-json";
        private readonly string workQueue = "rpsudp2json-vehicle-position-udp-raw";

        public RpsUdpToJsonWorker(Service serviceHost, ILogger<RpsUdpToJsonWorker> logger, IConfiguration config, IVehicleJourneyAssignmentCache vehicleJourneyAssignmentCache, UdpConverter udpConverter)
            : base(serviceHost, logger)
        {
            this.serviceHost = serviceHost;
            this.logger = logger;
            this.config = config;
            this.vehicleJourneyAssignmentCache = vehicleJourneyAssignmentCache;
            this.udpConverter = udpConverter;
        }

        protected override void Configure()
        {
            try
            {
                var url = config.GetValue<string>("RabbitMQ:Url");
                rabbitConnectionFactory = new ConnectionFactory() { Uri = new Uri(url) };
            }
            catch (Exception ex)
            {
                throw new ConfigurationException("Invalid configuration provided. Please confirm the RabbitMQ:Url configuration.", ex);
            }

            WatchDogTimeout = TimeSpan.FromMinutes(30);
        }

        protected override async Task Work(CancellationToken cancellationToken)
        {
            logger.LogInformation($"Connecting to RabbitMQ: {rabbitConnectionFactory.Uri}");
            rabbitConnection = rabbitConnectionFactory.CreateConnection(nameof(RpsUdpToJsonWorker));
            rabbitChannel = rabbitConnection.CreateModel();

            var arguments = new Dictionary<string, object>
            {
                { "x-message-ttl", 3 * 60 * 60 * 1000 } // 3 hours
            };
            rabbitChannel.ExchangeDeclare(jsonExchange, "fanout", durable: true, autoDelete: false);
            rabbitChannel.QueueDeclare(workQueue, durable: true, exclusive: false, autoDelete: false, arguments);
            rabbitChannel.QueueBind(workQueue, udpExchange, string.Empty);

            consumer = new EventingBasicConsumer(rabbitChannel);
            consumer.Received += (model, ea) =>
            {
                if (ea.Body.IsEmpty)
                {
                    logger.LogTrace("Recived empty position message");
                    return;
                }

                var bytes = ea.Body.ToArray();

                switch (bytes[0])
                {
                    case 2:
                        if (udpConverter.TryParseVehiclePosition(bytes, out var vehiclePositon))
                        {
                            if (vehicleJourneyAssignmentCache.TryGet(vehiclePositon.VehicleRef, out VehicleJourneyAssignment journeyAssignment) && journeyAssignment.InvalidFromUtc == null)
                                vehiclePositon.JourneyRef = journeyAssignment.JourneyRef;

                            var vehiclePositonEvent = new VehiclePositionEvent() { VehiclePosition = vehiclePositon };
                            var routingKey = $"{vehiclePositonEvent.EventType}.{vehiclePositon.VehicleRef}";

                            rabbitChannel.BasicPublish(jsonExchange, routingKey, body: Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(vehiclePositonEvent)));
                            ResetWatchdog();
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

            logger.LogInformation($"Beginning to consume {workQueue}...");

            rabbitChannel.BasicConsume(queue: workQueue,
                                       autoAck: true,
                                       consumer: consumer);

            await cancellationToken.WaitHandle.WaitOneAsync(CancellationToken.None);
        }

        protected override void Cleanup()
        {
            if (rabbitChannel != null)
                rabbitChannel.Close(200, "Goodbye");

            if (rabbitConnection != null)
                rabbitConnection.Close();
        }
    }
}
