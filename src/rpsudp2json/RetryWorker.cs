using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RpsUdpToJson
{
    public abstract class RetryWorker
    {
        private readonly ILogger logger;
        private readonly Service serviceHost;

        public TimeSpan WatchDogWakeupInterval { get; protected set; } = TimeSpan.FromSeconds(5);
        public TimeSpan WatchDogTimeout { get; protected set; } = TimeSpan.FromSeconds(30);
        public TimeSpan MaxDelay { get; protected set; } = TimeSpan.FromMinutes(15);

        private int retryAttempt = 0;
        private readonly Stopwatch watchDogTimer = new Stopwatch();
        private ManualResetEventSlim? manualRestart;

        public RetryWorker(Service serviceHost, ILogger logger)
        {
            this.logger = logger;
            this.serviceHost = serviceHost;
        }

        protected virtual void Configure() { }

        protected abstract Task Work(CancellationToken cancellationToken);

        private async Task WatchDog(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                /* Wait for manual reset event, or for watchdog timer to check message age. */
                manualRestart = new ManualResetEventSlim();
                var restart = await manualRestart.WaitHandle.WaitOneAsync(WatchDogWakeupInterval, cancellationToken);

                if (restart)
                {
                    logger.LogInformation($"ManualRestart was signaled. Watch dog is trying to restart...");
                    throw new ApplicationException("Watch dog triggered restart.");
                }
                else if (watchDogTimer.Elapsed > WatchDogTimeout)
                {
                    logger.LogInformation($"No new messages for {watchDogTimer.Elapsed}. Watch dog is trying to restart...");
                    throw new ApplicationException("Watch dog triggered restart.");
                }
            }
    }

        public async Task RetryWork()
        {
            try
            {
                Configure();
            }
            catch (Exception ex)
            {
                // We are likely not to recover from this exception. Stop service.
                logger.LogError(ex, "Applicaiton is not configured. Stopping service.");
                serviceHost.Stop();
                return;
            }

            while (!serviceHost.CancellationToken.IsCancellationRequested)
            {
                TimeSpan delay = TimeSpan.Zero;

                try
                {
                    var retryCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(serviceHost.CancellationToken);
                    watchDogTimer.Restart();
                                        
                    var workTask = Work(retryCancellationTokenSource.Token);
                    var watchDogTask = WatchDog(retryCancellationTokenSource.Token);
                    // Wait for either worker or watch dog to finish
                    var task = await Task.WhenAny(workTask, watchDogTask);
                    retryCancellationTokenSource.Cancel(); // Cancel the tasks
                    // In order to throw any exceptions from the tasks.
                    await workTask;
                    await watchDogTask;
                }
                catch (TaskCanceledException)
                {
                    // No-op
                }
                catch (Exception ex)
                {
                    retryAttempt++;
                    delay = TimeSpan.FromSeconds(Math.Pow(2, retryAttempt));
                    /* Cap delay of at max delay. */
                    if (delay > MaxDelay)
                        delay = MaxDelay;
                    /* Add small random delay to avoid all workers start exactly the same time. */
                    delay += TimeSpan.FromSeconds(new Random().Next(0, 5));

                    logger.LogError(ex, $"Worker failed. Retrying attempt {retryAttempt} in {delay}");
                }
                finally
                {
                    // Try to clean up nicely
                    Cleanup();
                }

                await Task.Delay(delay);
            }
        }

        protected void ResetWatchdog()
        {
            retryAttempt = 0;
            watchDogTimer.Restart();
        }

        protected virtual void Cleanup()
        {
            // No-op
        }
    }
}
