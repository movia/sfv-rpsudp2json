using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using Newtonsoft.Json;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;

using VehicleTracker.Contracts;

namespace RpsUdpToJson
{
    public class VehicleJourneyAssignmentLoaderWorker : RetryWorker
    {
        private const string clouldTableName = "VehicleJourneyAssignment";

        private readonly Service serviceHost;
        private readonly ILogger<VehicleJourneyAssignmentLoaderWorker> logger;
        private readonly IConfiguration config;
        private readonly IVehicleJourneyAssignmentCache vehicleJourneyAssignmentCache;

        private IConnection connection;
        private IModel channel;
        private EventingBasicConsumer consumer;

        public VehicleJourneyAssignmentLoaderWorker(Service serviceHost, ILogger<VehicleJourneyAssignmentLoaderWorker> logger, IConfiguration config, IVehicleJourneyAssignmentCache vehicleJourneyAssignmentCache)
            : base(serviceHost, logger)
        {
            this.serviceHost = serviceHost;
            this.logger = logger;
            this.config = config;
            this.vehicleJourneyAssignmentCache = vehicleJourneyAssignmentCache;

            WatchDogTimeout = TimeSpan.FromMinutes(30);
        }

        private void InitialLoad()
        {
            /* Load initial data from clould storage */
            CloudStorageAccount storageAccount;
            try
            {
                var storageConnectionString = config.GetValue<string>("ClouldStorage:ConnectionString");
                storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Invalid storage account information provided. Please confirm the ClouldStorage:ConnectionString configuration.");
                throw;
            }

            CloudTableClient tableClient = storageAccount.CreateCloudTableClient(new TableClientConfiguration());

            // Create the InsertOrReplace table operation
            var table = tableClient.GetTableReference(clouldTableName);
            TableContinuationToken token = null;
            var entities = new List<DynamicTableEntity>();
            do
            {
                var queryResult = table.ExecuteQuerySegmented(new TableQuery(), token);
                entities.AddRange(queryResult.Results);
                token = queryResult.ContinuationToken;
            } while (token != null);

            foreach (var item in entities)
            {
                var vehicleJourneyAssignment = new VehicleJourneyAssignment
                {
                    JourneyRef = item.Properties["JourneyRef"].StringValue,
                    VehicleRef = item.Properties["VehicleRef"].StringValue,
                    ValidFromUtc = item.Properties["ValidFromUtc"].DateTime.Value
                };

                if (item.Properties.TryGetValue("InvalidFromUtc", out var invalidFromUtc))
                    vehicleJourneyAssignment.InvalidFromUtc = invalidFromUtc.DateTime;

                vehicleJourneyAssignmentCache.Put(vehicleJourneyAssignment);
            }
        }

        protected override async Task Work()
        {
            var url = config.GetValue<string>("RabbitMQ:Url");
            logger.LogInformation($"Connecting to url: {url}");
            var factory = new ConnectionFactory() { Uri = new Uri(url) };
            connection = factory.CreateConnection();
            channel = connection.CreateModel();

            // The incomming exchange of ROI events.
            var roiExchange = config.GetValue<string>("RabbitMQ:RoiExchange", "roi-json");
            // The persistent queue is shared between workers, and coordinates persistent storage of vehicle assignments (in case of a single worker restarts).
            var persistentQueue = config.GetValue("RabbitMQ:RoiPersistentQueue", "rpsudp2json-roi-json-persistent");
            // The work queue is a per worker queue to distribute vehicle assignments.
            var workQueue = config.GetValue("RabbitMQ:RoiWorkQueue", "rpsudp2json-roi-json-{machineName}").Replace("{machineName}", Environment.MachineName.ToLower());

            var arguments = new Dictionary<string, object>
            {
                { "x-message-ttl", 3 * 60 * 60 * 1000 } // 3 hours
            };
            channel.QueueDeclare(persistentQueue, durable: true, exclusive: false, autoDelete: false, arguments);
            channel.QueueDeclare(workQueue, durable: false, exclusive: true, autoDelete: true, arguments);
            channel.QueueBind(persistentQueue, roiExchange, "vehicleJourneyAssignment.#");
            channel.QueueBind(workQueue, roiExchange, "vehicleJourneyAssignment.#");

            var workerVehicleJourneyAssignmentConsumer = new EventConsumer<VehicleJourneyAssignmentEvent>(
                action: e => vehicleJourneyAssignmentCache.Put(e.VehicleJourneyAssignment), 
                eventType: "vehicleJourneyAssignment",
                logger: logger);

            var persistentVehicleJourneyAssignmentQueue = new EventConsumer<VehicleJourneyAssignmentEvent>(
                action: e => vehicleJourneyAssignmentCache.Put(e.VehicleJourneyAssignment),
                eventType: "vehicleJourneyAssignment",
                logger: logger);

            /* This load initial vehicle journey assignments off clould storage. */
            try
            {
                InitialLoad();
            }
            catch
            {
                logger.LogWarning("Initial load of vehicle journey assignments failed. Continuing using only real-time data.");
            }

            logger.LogInformation($"Beginning to consume ...");

            channel.BasicConsume(
                queue: persistentQueue,
                autoAck: true,
                consumer: persistentVehicleJourneyAssignmentQueue);

            channel.BasicConsume(
                queue: workQueue,
                autoAck: true,
                consumer: workerVehicleJourneyAssignmentConsumer);

            await serviceHost.CancellationToken.WaitHandle.WaitOneAsync(CancellationToken.None);
        }
    }
}
