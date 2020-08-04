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
        private readonly CancellationToken cancellationToken;

        public TimeSpan WatchDogWakeupInterval { get; protected set; } = TimeSpan.FromSeconds(5);
        public TimeSpan WatchDogTimeout { get; protected set; } = TimeSpan.FromSeconds(30);
        public TimeSpan MaxDelay { get; protected set; } = TimeSpan.FromMinutes(15);

        private int retryAttempt = 0;
        private readonly Stopwatch watchDogTimer = new Stopwatch();
        private ManualResetEventSlim manualRestart;

        public RetryWorker(Service serviceHost, ILogger logger)
        {
            this.logger = logger;
            this.cancellationToken = serviceHost.CancellationToken;
        }

        protected abstract Task Work();

        private async Task WatchDog()
        {
            while (!cancellationToken.IsCancellationRequested)
            {               
                /* Wait for manual reset event, or for watchdog timer to check message age. */
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
            while (!cancellationToken.IsCancellationRequested)
            {
                TimeSpan delay = TimeSpan.Zero;

                try
                {
                    watchDogTimer.Restart();
                    manualRestart = new ManualResetEventSlim();
                    
                    var task = await Task.WhenAny(Work(), WatchDog());
                    await task; // In order to throw any exceptions from the task that terminated first.
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
