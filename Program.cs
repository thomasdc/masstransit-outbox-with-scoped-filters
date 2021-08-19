using System;
using System.Threading.Tasks;
using GreenPipes;
using MassTransit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;

namespace MassTransitOutboxWithScopedFilter
{
    public class Program
    {
        static async Task Main(string[] args)
        {
            var logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .Enrich.FromLogContext()
                .WriteTo.Console()
                .CreateLogger();

            var provider = new ServiceCollection()
                .AddLogging(_ => _.AddSerilog(logger))
                .AddMassTransit(x =>
                {
                    x.AddConsumer<SomeMessageConsumer>();
                    x.AddConsumer<SomeOtherMessageConsumer>();

                    x.UsingInMemory((context, cfg) =>
                    {
                        cfg.UseConsumeFilter(typeof(SomeConsumeFilter<>), context);
                        cfg.UsePublishFilter(typeof(SomePublishFilter<>), context);
                        cfg.UseInMemoryOutbox();
                        cfg.ConfigureEndpoints(context);
                    });
                }).BuildServiceProvider();

            var control = provider.GetRequiredService<IBusControl>();
            await control.StartAsync();

            logger.Information("Sending {MessageType}", nameof(ISomeMessage));
            await control.Publish<ISomeMessage>(new { });

            Console.ReadLine();

            await control.StopAsync();
        }
    }

    public interface ISomeMessage
    {
    }

    public interface ISomeOtherMessage
    {
    }

    public class SomeMessageConsumer : IConsumer<ISomeMessage>
    {
        private readonly ILogger<SomeMessageConsumer> _logger;

        public SomeMessageConsumer(ILogger<SomeMessageConsumer> logger)
        {
            _logger = logger;
        }

        public async Task Consume(ConsumeContext<ISomeMessage> context)
        {
            await Task.CompletedTask;
            _logger.LogInformation("Consuming ISomeMessage {@Message}", context.Message);
            await context.Publish<ISomeOtherMessage>(new { });
        }
    }

    public class SomeOtherMessageConsumer : IConsumer<ISomeOtherMessage>
    {
        private readonly ILogger<SomeOtherMessageConsumer> _logger;

        public SomeOtherMessageConsumer(ILogger<SomeOtherMessageConsumer> logger)
        {
            _logger = logger;
        }

        public async Task Consume(ConsumeContext<ISomeOtherMessage> context)
        {
            await Task.CompletedTask;
            _logger.LogInformation("Consuming ISomeOtherMessage {@Message}", context.Message);
        }
    }
    
    public class SomeConsumeFilter<T> : IFilter<ConsumeContext<T>> where T : class
    {
        private readonly ILogger<SomeConsumeFilter<T>> _logger;

        public SomeConsumeFilter(ILogger<SomeConsumeFilter<T>> logger)
        {
            _logger = logger;
        }

        public async Task Send(ConsumeContext<T> context, IPipe<ConsumeContext<T>> next)
        {
            _logger.LogInformation("*** BEFORE {ConsumerType} ({@Message})", nameof(SomeConsumeFilter<T>), context.Message);
            await next.Send(context);
            _logger.LogInformation("*** AFTER {ConsumerType} ({@Message})", nameof(SomeConsumeFilter<T>), context.Message);
        }

        public void Probe(ProbeContext context)
        {
        }
    }

    public class SomePublishFilter<T> : IFilter<PublishContext<T>> where T : class
    {
        private readonly ILogger<SomePublishFilter<T>> _logger;

        public SomePublishFilter(ILogger<SomePublishFilter<T>> logger)
        {
            _logger = logger;
        }

        public async Task Send(PublishContext<T> context, IPipe<PublishContext<T>> next)
        {
            _logger.LogInformation("*** BEFORE {ConsumerType} ({@Message})", nameof(SomePublishFilter<T>), context.Message);
            await next.Send(context);
            _logger.LogInformation("*** AFTER {ConsumerType} ({@Message})", nameof(SomePublishFilter<T>), context.Message);
        }

        public void Probe(ProbeContext context)
        {
        }
    }
}

