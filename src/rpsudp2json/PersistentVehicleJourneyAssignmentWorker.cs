using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Azure.Cosmos.Table;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using RabbitMQ.Client;

using VehicleTracker.Contracts;

namespace RpsUdpToJson
{
    public class PersistentVehicleJourneyAssignmentWorker : RetryWorker
    {
        private const string clouldTableName = "VehicleJourneyAssignment";

        private readonly Service serviceHost;
        private readonly ILogger<PersistentVehicleJourneyAssignmentWorker> logger;
        private readonly IConfiguration config;
        private readonly IVehicleJourneyAssignmentCache vehicleJourneyAssignmentCache;

        private CloudStorageAccount? storageAccount;
        private ConnectionFactory? rabbitConnectionFactory;
        private IConnection? rabbitConnection = null;
        private IModel? rabbitChannel = null;

        public PersistentVehicleJourneyAssignmentWorker(Service serviceHost, ILogger<PersistentVehicleJourneyAssignmentWorker> logger, IConfiguration config, IVehicleJourneyAssignmentCache vehicleJourneyAssignmentCache)
            : base(serviceHost, logger)
        {
            this.serviceHost = serviceHost;
            this.logger = logger;
            this.config = config;
            this.vehicleJourneyAssignmentCache = vehicleJourneyAssignmentCache;
        }

        protected override void Configure()
        {
            /* Load initial data from clould storage */
            try
            {
                var storageConnectionString = config.GetValue<string>("ClouldStorage:ConnectionString");
                storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            }
            catch (Exception ex)
            {
                throw new ConfigurationException("Invalid storage account information provided. Please confirm the ClouldStorage:ConnectionString configuration.", ex);
            }

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

        private void LoadFromPersistentStorage(CloudTable table)
        {
            TableContinuationToken? token = null;
            var entities = new List<DynamicTableEntity>();
            do
            {
                var queryResult = table.ExecuteQuerySegmented(new TableQuery(), token);
                entities.AddRange(queryResult.Results);
                token = queryResult.ContinuationToken;
            } while (token != null);

            foreach (var item in entities)
            {
                var vehicleJourneyAssignment = item.ToVehicleJourneyAssignment();
                vehicleJourneyAssignmentCache.Put(vehicleJourneyAssignment);
            }
        }

        protected override async Task Work(CancellationToken cancellationToken)
        {           
            logger.LogInformation($"Connecting to Azure Table Storage: {storageAccount.TableEndpoint}");
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient(new TableClientConfiguration());
            var table = tableClient.GetTableReference(clouldTableName);

            /* This load initial vehicle journey assignments off clould storage. */
            try
            {
                LoadFromPersistentStorage(table);
            }
            catch
            {
                logger.LogWarning("Initial load of vehicle journey assignments failed. Continuing using only real-time data.");
            }


            logger.LogInformation($"Connecting to RabbitMQ: {rabbitConnectionFactory.Uri}");
            var connection = rabbitConnectionFactory.CreateConnection(nameof(PersistentVehicleJourneyAssignmentWorker));
            var channel = connection.CreateModel();            

            // The incomming exchange of ROI events.
            var roiExchange = config.GetValue<string>("RabbitMQ:RoiExchange", "roi-json");
            // The persistent queue is shared between workers, and coordinates persistent storage of vehicle assignments (in case of a single worker restarts).
            var persistentQueue = config.GetValue("RabbitMQ:RoiPersistentQueue", "rpsudp2json-roi-json-persistent");

            var arguments = new Dictionary<string, object>
            {
                { "x-message-ttl", 3 * 60 * 60 * 1000 } // 3 hours
            };
            channel.QueueDeclare(persistentQueue, durable: true, exclusive: false, autoDelete: false, arguments);
            channel.QueueBind(persistentQueue, roiExchange, "vehicleJourneyAssignment.#");

            var persistentVehicleJourneyAssignmentQueue = new AsyncEventConsumer<VehicleJourneyAssignmentEvent>(
                action: async e =>
                {
                    await table.ExecuteAsync(
                        TableOperation.InsertOrReplace(e.VehicleJourneyAssignment.ToTableEntity())
                    );
                    ResetWatchdog();
                },
                eventType: "vehicleJourneyAssignment",
                logger: logger);

            logger.LogInformation($"Beginning to consume {persistentQueue} ...");

            var persistentConsumerTag = channel.BasicConsume(
                queue: persistentQueue,
                autoAck: true,
                consumer: persistentVehicleJourneyAssignmentQueue);

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
