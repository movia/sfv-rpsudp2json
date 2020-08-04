using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

namespace RpsUdpToJson
{
    public class Service
    {
        private readonly ILogger<Service> logger;
        private readonly IServiceProvider serviceProvider;

        private readonly CancellationTokenSource cancellationTokenSource;        
        private readonly List<Task> tasks;

        public CancellationToken CancellationToken => cancellationTokenSource.Token;

        public Service(ILogger<Service> logger, IServiceProvider serviceProvider)
        {
            this.logger = logger;
            this.serviceProvider = serviceProvider;

            cancellationTokenSource = new CancellationTokenSource();
            tasks = new List<Task>();
        }

        private void AddWorker<T>(string name = null) where T : RetryWorker
        {
            name ??= typeof(T).Name;
            logger.LogInformation($"Service is starting worker '{name}'");
            var worker = (T)serviceProvider.GetService(typeof(T));
            var task = Task.Factory.StartNew(async () => await worker.RetryWork(), TaskCreationOptions.LongRunning);
            tasks.Add(task);
        }

        public void Start()
        {
            logger.LogInformation("Service is starting.");
            AddWorker<VehicleJourneyAssignmentLoaderWorker>();
            AddWorker<RpsUdpToJsonWorker>();
        }

        public void Stop()
        {
            logger.LogInformation("Service is stopping.");
            cancellationTokenSource.Cancel();
            Task.WaitAll(tasks.ToArray(), 1000);
        }
    }
}
