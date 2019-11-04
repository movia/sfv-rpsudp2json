using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RpsUdpToJson
{
    public class Service
    {
        private readonly ILogger<Service> logger;
        private readonly IConfiguration config;
        private readonly IServiceProvider serviceProvider;

        private readonly CancellationTokenSource cancellationTokenSource;        
        private readonly List<Task> tasks;

        public CancellationToken CancellationToken => cancellationTokenSource.Token;

        public Service(ILogger<Service> logger, IConfiguration config, IServiceProvider serviceProvider)
        {
            this.logger = logger;
            this.config = config;
            this.serviceProvider = serviceProvider;

            cancellationTokenSource = new CancellationTokenSource();
            tasks = new List<Task>();
        }

        private void AddWorker<T>(string name = null) where T : RetryWorker
        {
            name = name ?? typeof(T).Name;
            logger.LogInformation($"Service is starting worker '{name}'");
            var worker = (T)serviceProvider.GetService(typeof(T));
            var task = Task.Factory.StartNew(async () => await worker.RetryWork(), TaskCreationOptions.LongRunning);
            tasks.Add(task);

        }

        public void Start()
        {
            logger.LogInformation("Service is starting.");
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
