using System;
using System.Threading;
using System.Threading.Tasks;
using ActiveMQ.Artemis.Client.Exceptions;
using ActiveMQ.Artemis.Client.Extensions.DependencyInjection;
using ActiveMQ.Artemis.Client.Transactions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace ActiveMQ.Artemis.Client.UnitTests
{
    public class ActiveMqConsumerRecoverySpec
    {
        [Fact]
        public async Task Hosted_consumer_retries_creation_after_consumer_closed_exception()
        {
            var closedConsumer = new TestConsumer(throwOnFirstReceive: true);
            var recoveredConsumer = new TestConsumer(throwOnFirstReceive: false);
            var factoryCalls = 0;
            var messageHandled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var observable = new ContextualReceiveObservable(new ReceiveObservable("test", Array.Empty<IReceiveObserver>()));
            var activeMqConsumer = new ActiveMqConsumer(
                new TestServiceProvider(),
                observable,
                async _ =>
                {
                    var attempt = Interlocked.Increment(ref factoryCalls);
                    if (attempt == 1)
                    {
                        return closedConsumer;
                    }

                    if (attempt == 2)
                    {
                        throw new InvalidOperationException("The queue is not available yet.");
                    }

                    return recoveredConsumer;
                },
                (_, _, _, _) =>
                {
                    messageHandled.TrySetResult(true);
                    return Task.CompletedTask;
                });

            using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await activeMqConsumer.StartAsync(cancellation.Token);
            await messageHandled.Task.WaitAsync(TimeSpan.FromSeconds(7));

            Assert.Equal(3, Volatile.Read(ref factoryCalls));
            Assert.True(closedConsumer.IsDisposed);

            await activeMqConsumer.StopAsync();

            Assert.True(recoveredConsumer.IsDisposed);
        }

        [Fact]
        public async Task Stop_during_recreate_disposes_replacement_and_does_not_leak()
        {
            var closedConsumer = new TestConsumer(throwOnFirstReceive: true);
            var factorySecondCallStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var factoryUnblock = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var factoryCalls = 0;
            IConsumer replacement = null;
            var observable = new ContextualReceiveObservable(new ReceiveObservable("test", Array.Empty<IReceiveObserver>()));
            var activeMqConsumer = new ActiveMqConsumer(
                new TestServiceProvider(),
                observable,
                async token =>
                {
                    var attempt = Interlocked.Increment(ref factoryCalls);
                    if (attempt == 1)
                    {
                        return closedConsumer;
                    }

                    factorySecondCallStarted.TrySetResult(true);
                    using var registration = token.Register(() => factoryUnblock.TrySetCanceled());
                    await factoryUnblock.Task;
                    replacement = new TestConsumer(throwOnFirstReceive: false);
                    return replacement;
                },
                (_, _, _, _) => Task.CompletedTask,
                ActiveMQ.Artemis.Client.AutoRecovering.RecoveryPolicy.RecoveryPolicyFactory.ConstantBackoff(TimeSpan.FromMilliseconds(10)));

            using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await activeMqConsumer.StartAsync(cancellation.Token);
            await factorySecondCallStarted.Task.WaitAsync(TimeSpan.FromSeconds(7));

            var stopTask = activeMqConsumer.StopAsync();
            var completed = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(7)));
            Assert.Same(stopTask, completed);
            await stopTask;

            Assert.True(closedConsumer.IsDisposed);
            // Either the replacement was never swapped (still null) or it was swapped
            // and then disposed by StopAsync / the losing Recreate race.
            if (replacement != null)
            {
                Assert.True(((TestConsumer)replacement).IsDisposed);
            }

            Assert.Equal(2, Volatile.Read(ref factoryCalls));
        }

        private sealed class TestServiceProvider : IServiceProvider
        {
            public object GetService(Type serviceType)
            {
                return serviceType == typeof(ILogger<ActiveMqConsumer>)
                    ? NullLogger<ActiveMqConsumer>.Instance
                    : null;
            }
        }

        private sealed class TestConsumer : IConsumer
        {
            private readonly bool _throwOnFirstReceive;
            private int _receiveCount;

            public TestConsumer(bool throwOnFirstReceive)
            {
                _throwOnFirstReceive = throwOnFirstReceive;
            }

            public bool IsDisposed { get; private set; }

            public ValueTask<Message> ReceiveAsync(CancellationToken cancellationToken = default)
            {
                if (_throwOnFirstReceive && Interlocked.Increment(ref _receiveCount) == 1)
                {
                    return ValueTask.FromException<Message>(new ConsumerClosedException());
                }

                if (!_throwOnFirstReceive && Interlocked.Increment(ref _receiveCount) == 1)
                {
                    return new ValueTask<Message>(new Message("recovered"));
                }

                return new ValueTask<Message>(WaitForCancellationAsync(cancellationToken));
            }

            public ValueTask AcceptAsync(Message message, Transaction transaction, CancellationToken cancellationToken = default) => default;
            public void Modify(Message message, bool deliveryFailed, bool undeliverableHere) { }
            public void Reject(Message message) { }

            public ValueTask DisposeAsync()
            {
                IsDisposed = true;
                return default;
            }

            private static async Task<Message> WaitForCancellationAsync(CancellationToken cancellationToken)
            {
                await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
                return null;
            }
        }
    }
}
