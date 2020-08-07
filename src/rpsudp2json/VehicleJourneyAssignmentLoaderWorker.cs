using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using RabbitMQ.Client;

using VehicleTracker.Contracts;

namespace RpsUdpToJson
{
    public class VehicleJourneyAssignmentLoaderWorker : RetryWorker
    {
        private readonly Service serviceHost;
        private readonly ILogger<VehicleJourneyAssignmentLoaderWorker> logger;
        private readonly IConfiguration config;
        private readonly IVehicleJourneyAssignmentCache vehicleJourneyAssignmentCache;

        private ConnectionFactory? rabbitConnectionFactory;
        private IConnection? rabbitConnection = null;
        private IModel? rabbitChannel = null;

        public VehicleJourneyAssignmentLoaderWorker(Service serviceHost, ILogger<VehicleJourneyAssignmentLoaderWorker> logger, IConfiguration config, IVehicleJourneyAssignmentCache vehicleJourneyAssignmentCache)
            : base(serviceHost, logger)
        {
            this.serviceHost = serviceHost;
            this.logger = logger;
            this.config = config;
            this.vehicleJourneyAssignmentCache = vehicleJourneyAssignmentCache;
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
            rabbitConnection = rabbitConnectionFactory.CreateConnection(nameof(VehicleJourneyAssignmentLoaderWorker));
            rabbitChannel = rabbitConnection.CreateModel();

            // The incomming exchange of ROI events.
            var roiExchange = config.GetValue<string>("RabbitMQ:RoiExchange", "roi-json");
            // The work queue is a per worker queue to distribute vehicle assignments.
            var workQueue = config.GetValue("RabbitMQ:RoiWorkQueue", "rpsudp2json-roi-json-{machineName}").Replace("{machineName}", Environment.MachineName.ToLower());

            var arguments = new Dictionary<string, object>
            {
                { "x-message-ttl", 3 * 60 * 60 * 1000 } // 3 hours
            };
            rabbitChannel.QueueDeclare(workQueue, durable: false, exclusive: true, autoDelete: true, arguments);
            rabbitChannel.QueueBind(workQueue, roiExchange, "vehicleJourneyAssignment.#");

            var workerVehicleJourneyAssignmentConsumer = new EventConsumer<VehicleJourneyAssignmentEvent>(
                action: e =>
                {
                    vehicleJourneyAssignmentCache.Put(e.VehicleJourneyAssignment);
                    ResetWatchdog();
                },
                eventType: "vehicleJourneyAssignment",
                logger: logger);

            // TODO: Wait for initial load to complete in PersistentVehicleJourneyAssignmentWorker

            logger.LogInformation($"Beginning to consume {workQueue}...");

            rabbitChannel.BasicConsume(
                queue: workQueue,
                autoAck: true,
                consumer: workerVehicleJourneyAssignmentConsumer);

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
