using System.Threading;
using System.Threading.Tasks;
using ActiveMQ.Artemis.Client.Extensions.HealthChecks;
using ActiveMQ.Artemis.Client.Transactions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Xunit;

namespace ActiveMQ.Artemis.Client.UnitTests
{
    public class ArtemisHealthCheckServiceSpec
    {
        [Fact]
        public async Task Consumer_failure_keeps_health_unhealthy_until_that_consumer_is_recovered()
        {
            var service = new ArtemisHealthCheckService();
            var consumer = new TestConsumer();
            service.ConnectionOpen();

            service.ConsumerStateChanged(null, new ConsumerLifecycleEventArgs(consumer, ConsumerLifecycleState.Unavailable));
            Assert.Equal(HealthStatus.Unhealthy, (await service.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None)).Status);

            service.ConnectionRecovered(null, new ConnectionRecoveredEventArgs(Endpoint.Create("localhost", 5672)));
            Assert.Equal(HealthStatus.Unhealthy, (await service.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None)).Status);

            service.ConsumerStateChanged(null, new ConsumerLifecycleEventArgs(consumer, ConsumerLifecycleState.Recovered));
            Assert.Equal(HealthStatus.Healthy, (await service.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None)).Status);
        }

        [Fact]
        public async Task Disposed_unavailable_consumer_is_removed_from_health_state()
        {
            var service = new ArtemisHealthCheckService();
            var consumer = new TestConsumer();
            service.ConnectionOpen();

            service.ConsumerStateChanged(null, new ConsumerLifecycleEventArgs(consumer, ConsumerLifecycleState.Unavailable));
            service.ConsumerStateChanged(null, new ConsumerLifecycleEventArgs(consumer, ConsumerLifecycleState.Disposed));

            Assert.Equal(HealthStatus.Healthy, (await service.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None)).Status);
        }

        [Fact]
        public async Task Failed_consumer_recovery_keeps_health_unhealthy()
        {
            var service = new ArtemisHealthCheckService();
            var consumer = new TestConsumer();
            service.ConnectionOpen();

            service.ConsumerStateChanged(null, new ConsumerLifecycleEventArgs(consumer, ConsumerLifecycleState.RecoveryFailed));

            Assert.Equal(HealthStatus.Unhealthy, (await service.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None)).Status);
        }

        private sealed class TestConsumer : IConsumer
        {
            public ValueTask<Message> ReceiveAsync(CancellationToken cancellationToken = default) => default;
            public ValueTask AcceptAsync(Message message, Transaction transaction, CancellationToken cancellationToken = default) => default;
            public void Modify(Message message, bool deliveryFailed, bool undeliverableHere) { }
            public void Reject(Message message) { }
            public ValueTask DisposeAsync() => default;
        }
    }
}
