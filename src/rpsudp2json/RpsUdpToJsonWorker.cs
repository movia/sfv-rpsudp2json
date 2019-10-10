using System;
using System.Threading;
using System.Collections.Generic;
using System.Text;
using Polly;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using Microsoft.Extensions.Configuration;
using RabbitMQ.Client.Events;
using System.Threading.Tasks;
using VehicleTracker.Contracts;
using Newtonsoft.Json;

namespace RpsUdpToJson
{
    public class RpsUdpToJsonWorker
    {
        private ILogger<RpsUdpToJsonWorker> logger;
        private IConfiguration config;
        private UdpConverter udpConverter;

        private CancellationTokenSource cancellationTokenSource;
        private CancellationToken cancellationToken;

        public RpsUdpToJsonWorker(ILogger<RpsUdpToJsonWorker> logger, IConfiguration config, UdpConverter udpConverter)
        {
            this.logger = logger;
            this.config = config;
            this.udpConverter = udpConverter;
        }

        private async Task Work()
        {
            var url = config.GetSection("RabbitMQ")?.GetValue<string>("Url");
            logger.LogInformation($"Connecting to url: {url}");
            var factory = new ConnectionFactory() { Uri = new Uri(url) };
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            var udpExchange = "vehicle-position-udp-raw";
            var jsonExchange = "vehicle-position-json";
            var workQueue = "rpsudp2json-vehicle-position-udp-raw";

            var arguments = new Dictionary<string, object>();
            arguments.Add("x-message-ttl", 3 * 60 * 60 * 1000); // 3 hours
            channel.ExchangeDeclare(jsonExchange, "fanout", durable: true, autoDelete: false);
            channel.QueueDeclare(workQueue, durable: true, exclusive: false, autoDelete: false, arguments);
            channel.QueueBind(workQueue, udpExchange, string.Empty);

            var consumer = new EventingBasicConsumer(channel);
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
            channel.BasicConsume(queue: workQueue,
                                 autoAck: true,
                                 consumer: consumer);

            await Task.Run(() => cancellationToken.WaitHandle.WaitOne());
            logger.LogInformation($"Cancel consumer ({consumer.ConsumerTag})");
            channel.BasicCancel(consumer.ConsumerTag);
        }

        public void Start()
        {
            cancellationTokenSource = new CancellationTokenSource();
            cancellationToken = cancellationTokenSource.Token;

            Policy
                .Handle<Exception>()
                .WaitAndRetryForever(retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (ex, retryCount, timeSpan) => logger.LogError(ex, $"Worker failed. Retrying attempt {retryCount} in {timeSpan}"))
                .Execute(Work);
        }

        public void Stop()
        {
            cancellationTokenSource.Cancel();
        }
    }
}
