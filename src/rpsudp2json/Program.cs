using System;
using System.IO;
using System.Threading.Tasks;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using NLog;
using NLog.Extensions.Logging;

using Topshelf;
using VehicleTracker.Contracts;

namespace RpsUdpToJson
{
    class Program
    {
        private static IServiceProvider ConfigureServices(IConfiguration config)
        {            
            return new ServiceCollection()
               .AddSingleton(config)
               .AddLogging(loggingBuilder =>
               {
                   // configure Logging with NLog
                   loggingBuilder.ClearProviders();
                   loggingBuilder.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Trace);
                   loggingBuilder.AddNLog("nlog.config");
               })
               .AddTransient<UdpConverter>()
               .AddTransient<RpsUdpToJsonWorker>()
               .AddTransient<VehicleJourneyAssignmentLoaderWorker>()
               .AddSingleton<IVehicleJourneyAssignmentCache, InMemoryVehicleJourneyAssignmentCache>()
               .AddSingleton<Service>()
               .BuildServiceProvider();
        }

        public static void Main()
        {
            var logger = LogManager.GetCurrentClassLogger();
            try
            {
                var environmentName = Environment.GetEnvironmentVariable("ENVIRONMENT");

                var config = new ConfigurationBuilder()
                   .SetBasePath(Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location))
                   .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                   .AddJsonFile($"appsettings.{environmentName}.json", optional: true, reloadOnChange: true)
                   .Build();

                var servicesProvider = ConfigureServices(config);
                using (servicesProvider as IDisposable)
                {
                    var rc = HostFactory.Run(x =>
                    {
                        x.Service<Service>(s => {
                            s.ConstructUsing(name => servicesProvider.GetService<Service>());
                            s.WhenStarted(tc => tc.Start());
                            s.WhenStopped(tc => tc.Stop());
                        });
                        x.UseNLog();
                        x.RunAsLocalSystem();

                        x.SetDescription("Convert RPS UDP format to JSON");
                        x.SetDisplayName("RPS UDP to JSON");
                        x.SetServiceName("rpsudp2json");
                    });

                    var exitCode = (int)Convert.ChangeType(rc, rc.GetTypeCode());
                    Environment.ExitCode = exitCode;
                }
            }
            catch (Exception ex)
            {
                // NLog: catch any exception and log it.
                logger.Error(ex, "Stopped program because of exception");
                throw;
            }
            finally
            {
                // Ensure to flush and stop internal timers/threads before application-exit (Avoid segmentation fault on Linux)
                LogManager.Shutdown();
            }
        }
    }
}
