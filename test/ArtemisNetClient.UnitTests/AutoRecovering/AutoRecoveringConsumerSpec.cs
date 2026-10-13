using System;
using System.Threading;
using System.Threading.Tasks;
using ActiveMQ.Artemis.Client.AutoRecovering.RecoveryPolicy;
using ActiveMQ.Artemis.Client.Exceptions;
using ActiveMQ.Artemis.Client.InternalUtilities;
using ActiveMQ.Artemis.Client.UnitTests.Utils;
using Amqp;
using Amqp.Framing;
using Amqp.Handler;
using Amqp.Listener;
using Xunit;
using Xunit.Abstractions;

namespace ActiveMQ.Artemis.Client.UnitTests.AutoRecovering
{
    public class AutoRecoveringConsumerSpec : ActiveMQNetSpec
    {
        public AutoRecoveringConsumerSpec(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Should_be_able_to_receive_messages_when_connection_restored()
        {
            var (consumer, messageSource, host, connection) = await CreateReattachedConsumer();

            messageSource.Enqueue(new Message("foo"));

            var cts = new CancellationTokenSource(Timeout);
            var message = await consumer.ReceiveAsync(cts.Token);
            Assert.NotNull(message);
            Assert.Equal("foo", message.GetBody<string>());

            await DisposeUtil.DisposeAll(consumer, connection, host);
        }

        [Fact]
        public async Task Should_be_able_to_receive_message_when_connection_restored_after_receive_called()
        {
            var host1 = CreateOpenedContainerHost();
            var host2 = CreateContainerHost();

            var connection = await CreateConnection(new[] { host1.Endpoint, host2.Endpoint });
            var consumer = await connection.CreateConsumerAsync("a1", RoutingType.Anycast);
            
            var cts = new CancellationTokenSource(Timeout);
            var receiveTask = consumer.ReceiveAsync(cts.Token);

            host2.Open();
            host1.Dispose();

            var messageSource = host2.CreateMessageSource("a1");
            messageSource.Enqueue(new Message("foo"));

            var message = await receiveTask;
            Assert.NotNull(message);
            Assert.Equal("foo", message.GetBody<string>());

            await DisposeUtil.DisposeAll(consumer, connection, host2);
        }

        [Fact]
        public async Task Should_be_able_to_accept_messages_when_connection_restored()
        {
            var (consumer, messageSource, host, connection) = await CreateReattachedConsumer();

            messageSource.Enqueue(new Message("foo"));

            var cts = new CancellationTokenSource(Timeout);
            var message = await consumer.ReceiveAsync(cts.Token);
            await consumer.AcceptAsync(message);

            var dispositionContext = messageSource.GetNextDisposition(Timeout);
            Assert.IsType<Accepted>(dispositionContext.DeliveryState);
            Assert.True(dispositionContext.Settled);
            
            await DisposeUtil.DisposeAll(consumer, connection, host);
        }

        [Fact]
        public async Task Should_be_able_to_receive_and_reject_messages_when_connection_restored()
        {
            var (consumer, messageSource, host, connection) = await CreateReattachedConsumer();

            messageSource.Enqueue(new Message("foo"));

            var cts = new CancellationTokenSource(Timeout);
            var message = await consumer.ReceiveAsync(cts.Token);
            consumer.Reject(message);

            var dispositionContext = messageSource.GetNextDisposition(Timeout);
            Assert.IsType<Rejected>(dispositionContext.DeliveryState);
            Assert.True(dispositionContext.Settled);
            
            await DisposeUtil.DisposeAll(consumer, connection, host);
        }

        [Fact]
        public async Task Should_be_able_to_modify_messages_when_connection_restored()
        {
            var (consumer, messageSource, host, connection) = await CreateReattachedConsumer();

            messageSource.Enqueue(new Message("foo"));

            var cts = new CancellationTokenSource(Timeout);
            var message = await consumer.ReceiveAsync(cts.Token);
            consumer.Modify(message, deliveryFailed: true, undeliverableHere: false);

            var dispositionContext = messageSource.GetNextDisposition(Timeout);
            Assert.IsType<Modified>(dispositionContext.DeliveryState);
            Assert.True(dispositionContext.Settled);
            
            await DisposeUtil.DisposeAll(consumer, connection, host);
        }
        
        [Fact]
        public async Task Throws_when_recovery_policy_gave_up_and_consumer_was_not_able_to_receive_message_if_ReceiveAsync_called_after_connection_lost()
        {
            var endpoint = GetUniqueEndpoint();

            var host = CreateOpenedContainerHost(endpoint);

            var connectionFactory = CreateConnectionFactory();
            connectionFactory.RecoveryPolicy = RecoveryPolicyFactory.ConstantBackoff(TimeSpan.FromMilliseconds(100), 1);

            await using var connection = await connectionFactory.CreateAsync(endpoint);

            var consumer = await connection.CreateConsumerAsync("a1", RoutingType.Anycast);

            await DisposeHostAndWaitUntilConnectionNotified(host, connection);

            var cts = new CancellationTokenSource(Timeout);
            await Assert.ThrowsAsync<ConsumerClosedException>(async() => await consumer.ReceiveAsync(cts.Token));
        }
        
        [Fact]
        public async Task Throws_when_recovery_policy_gave_up_and_consumer_was_not_able_to_receive_message_if_ReceiveAsync_called_before_connection_lost()
        {
            var endpoint = GetUniqueEndpoint();

            var host = CreateOpenedContainerHost(endpoint);

            var connectionFactory = CreateConnectionFactory();
            connectionFactory.RecoveryPolicy = RecoveryPolicyFactory.ConstantBackoff(TimeSpan.FromMilliseconds(100), 1);

            await using var connection = await connectionFactory.CreateAsync(endpoint);

            var consumer = await connection.CreateConsumerAsync("a1", RoutingType.Anycast);
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            var receiveTask = consumer.ReceiveAsync(cts.Token);

            await DisposeHostAndWaitUntilConnectionNotified(host, connection);

            await Assert.ThrowsAsync<ConsumerClosedException>(async () => await receiveTask);
        }

        [Fact]
        public async Task Waits_for_resource_deleted_consumer_to_become_available_and_recreates_it()
        {
            var host = CreateOpenedContainerHost();
            var linkProcessor = host.CreateTestLinkProcessor();
            var unavailable = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var recovered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var retryStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var attachCount = 0;
            var queueAvailable = 0;
            ListenerLink initialLink = null;

            linkProcessor.SetHandler(context =>
            {
                var attempt = Interlocked.Increment(ref attachCount);
                if (attempt == 1)
                {
                    initialLink = context.Link;
                    return false;
                }

                retryStarted.TrySetResult(true);
                if (Volatile.Read(ref queueAvailable) == 0)
                {
                    context.Complete(new Error(ErrorCode.NotFound) { Description = "Queue: 'a1' does not exist" });
                    return true;
                }

                context.Complete(new TestLinkEndpoint(), context.Attach.Role ? 0 : 30);
                return true;
            });

            var connectionFactory = CreateConnectionFactory();
            connectionFactory.RecoveryPolicy = RecoveryPolicyFactory.ConstantBackoff(TimeSpan.FromMilliseconds(20));
            await using var connection = await connectionFactory.CreateAsync(host.Endpoint);
            ((IConsumerLifecycleEvents)connection).ConsumerStateChanged += (_, args) =>
            {
                if (args.State == ConsumerLifecycleState.Unavailable)
                {
                    unavailable.TrySetResult(true);
                }
                else if (args.State == ConsumerLifecycleState.Recovered)
                {
                    recovered.TrySetResult(true);
                }
            };

            await using var consumer = await connection.CreateConsumerAsync("a1", RoutingType.Anycast);
            using var cancellation = new CancellationTokenSource(Timeout);
            var receiveTask = consumer.ReceiveAsync(cancellation.Token);

            try
            {
                await initialLink.CloseAsync(Timeout, new Error(ErrorCode.ResourceDeleted) { Description = "Queue was deleted: a1" });
            }
            catch (Exception)
            {
            }

            await unavailable.Task.WaitAsync(Timeout);
            await retryStarted.Task.WaitAsync(Timeout);
            await Task.Delay(TimeSpan.FromMilliseconds(100));
            Assert.False(recovered.Task.IsCompleted);

            Volatile.Write(ref queueAvailable, 1);
            await recovered.Task.WaitAsync(Timeout);
            Assert.True(Volatile.Read(ref attachCount) >= 3);

            cancellation.Cancel();
            await Assert.ThrowsAsync<TaskCanceledException>(async () => await receiveTask);
        }

        [Fact]
        public async Task Duplicate_close_signals_are_coalesced_into_single_recovery()
        {
            var host = CreateOpenedContainerHost();
            var linkProcessor = host.CreateTestLinkProcessor();
            var attachCount = 0;
            var unavailableCount = 0;
            var recoveredCount = 0;
            ListenerLink initialLink = null;

            linkProcessor.SetHandler(context =>
            {
                var attempt = Interlocked.Increment(ref attachCount);
                if (attempt == 1)
                {
                    initialLink = context.Link;
                    return false;
                }

                context.Complete(new TestLinkEndpoint(), context.Attach.Role ? 0 : 30);
                return true;
            });

            var connectionFactory = CreateConnectionFactory();
            connectionFactory.RecoveryPolicy = RecoveryPolicyFactory.ConstantBackoff(TimeSpan.FromMilliseconds(20));
            await using var connection = await connectionFactory.CreateAsync(host.Endpoint);
            ((IConsumerLifecycleEvents)connection).ConsumerStateChanged += (_, args) =>
            {
                if (args.State == ConsumerLifecycleState.Unavailable)
                {
                    Interlocked.Increment(ref unavailableCount);
                }
                else if (args.State == ConsumerLifecycleState.Recovered)
                {
                    Interlocked.Increment(ref recoveredCount);
                }
            };

            await using var consumer = await connection.CreateConsumerAsync("a1", RoutingType.Anycast);
            using var cancellation = new CancellationTokenSource(Timeout);
            // Hold ReceiveAsync so both the channel-completion path and the
            // underlying-link lifecycle event fire for the same close.
            var receiveTask = consumer.ReceiveAsync(cancellation.Token);

            try
            {
                await initialLink.CloseAsync(Timeout, new Error(ErrorCode.ResourceDeleted) { Description = "Queue was deleted: a1" });
            }
            catch (Exception)
            {
            }

            // Wait for recovery, then give duplicates a chance to cause extra attaches.
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
            while (Volatile.Read(ref recoveredCount) == 0 && DateTime.UtcNow < deadline)
            {
                await Task.Delay(20);
            }

            Assert.Equal(1, Volatile.Read(ref recoveredCount));
            await Task.Delay(200);
            Assert.Equal(1, Volatile.Read(ref unavailableCount));
            Assert.Equal(2, Volatile.Read(ref attachCount));

            cancellation.Cancel();
            await Assert.ThrowsAsync<OperationCanceledException>(async () => await receiveTask);
        }

        [Fact]
        public async Task Dispose_during_consumer_backoff_cancels_recovery()
        {
            var host = CreateOpenedContainerHost();
            var linkProcessor = host.CreateTestLinkProcessor();
            var attachCount = 0;
            var unavailable = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            ListenerLink initialLink = null;

            linkProcessor.SetHandler(context =>
            {
                var attempt = Interlocked.Increment(ref attachCount);
                if (attempt == 1)
                {
                    initialLink = context.Link;
                    return false;
                }

                context.Complete(new Error(ErrorCode.NotFound) { Description = "Queue: 'a1' does not exist" });
                return true;
            });

            var connectionFactory = CreateConnectionFactory();
            connectionFactory.RecoveryPolicy = RecoveryPolicyFactory.ConstantBackoff(TimeSpan.FromSeconds(30));
            await using var connection = await connectionFactory.CreateAsync(host.Endpoint);
            ((IConsumerLifecycleEvents)connection).ConsumerStateChanged += (_, args) =>
            {
                if (args.State == ConsumerLifecycleState.Unavailable)
                {
                    unavailable.TrySetResult(true);
                }
            };

            var consumer = await connection.CreateConsumerAsync("a1", RoutingType.Anycast);
            using var cancellation = new CancellationTokenSource(Timeout);
            var receiveTask = consumer.ReceiveAsync(cancellation.Token);

            try
            {
                await initialLink.CloseAsync(Timeout, new Error(ErrorCode.ResourceDeleted) { Description = "Queue was deleted: a1" });
            }
            catch (Exception)
            {
            }

            await unavailable.Task.WaitAsync(Timeout);

            // Wait until the first recovery attempt has failed (second attach with
            // NotFound) so the 30s backoff is pending. Disposing must cancel the
            // pending retry, not wait for it.
            var deadline = DateTime.UtcNow + Timeout;
            while (Volatile.Read(ref attachCount) < 2 && DateTime.UtcNow < deadline)
            {
                await Task.Delay(20);
            }

            Assert.True(Volatile.Read(ref attachCount) >= 2);
            await Task.Delay(200);

            // Dispose must not wait for the 30s backoff.
            var disposeTask = consumer.DisposeAsync().AsTask();
            var completed = await Task.WhenAny(disposeTask, Task.Delay(Timeout));
            Assert.Same(disposeTask, completed);
            await disposeTask;

            var attachesAfterDispose = Volatile.Read(ref attachCount);
            await Task.Delay(300);
            Assert.Equal(attachesAfterDispose, Volatile.Read(ref attachCount));

            cancellation.Cancel();
            try
            {
                await receiveTask;
            }
            catch (Exception)
            {
                // Receive unblocks with cancellation or disposal; either is acceptable here.
            }

            await connection.DisposeAsync();
            host.Dispose();
        }

        [Fact]
        public async Task Recovery_policy_give_up_terminates_consumer()
        {
            var host = CreateOpenedContainerHost();
            var linkProcessor = host.CreateTestLinkProcessor();
            var recoveryFailed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            ListenerLink initialLink = null;
            var attachCount = 0;

            linkProcessor.SetHandler(context =>
            {
                var attempt = Interlocked.Increment(ref attachCount);
                if (attempt == 1)
                {
                    initialLink = context.Link;
                    return false;
                }

                context.Complete(new Error(ErrorCode.NotFound) { Description = "Queue: 'a1' does not exist" });
                return true;
            });

            var connectionFactory = CreateConnectionFactory();
            connectionFactory.RecoveryPolicy = RecoveryPolicyFactory.ConstantBackoff(TimeSpan.FromMilliseconds(20), retryCount: 1);
            await using var connection = await connectionFactory.CreateAsync(host.Endpoint);
            ((IConsumerLifecycleEvents)connection).ConsumerStateChanged += (_, args) =>
            {
                if (args.State == ConsumerLifecycleState.RecoveryFailed)
                {
                    recoveryFailed.TrySetResult(true);
                }
            };

            await using var consumer = await connection.CreateConsumerAsync("a1", RoutingType.Anycast);
            using var cancellation = new CancellationTokenSource(Timeout);
            var receiveTask = consumer.ReceiveAsync(cancellation.Token);

            try
            {
                await initialLink.CloseAsync(Timeout, new Error(ErrorCode.ResourceDeleted) { Description = "Queue was deleted: a1" });
            }
            catch (Exception)
            {
            }

            await recoveryFailed.Task.WaitAsync(Timeout);
            await Assert.ThrowsAsync<ConsumerClosedException>(async () => await receiveTask);
        }

        [Fact]
        public async Task Should_recreate_consumers_on_connection_recovery()
        {
            var endpoint = GetUniqueEndpoint();
            var consumersAttached = new CountdownEvent(2);
            var testHandler = new TestHandler(@event =>
            {
                switch (@event.Id)
                {
                    case EventId.LinkRemoteOpen when @event.Context is Attach attach && !attach.Role:
                        consumersAttached.Signal();
                        break;
                }
            });

            var host1 = CreateOpenedContainerHost(endpoint, testHandler);

            var connection = await CreateConnection(endpoint);
            await connection.CreateConsumerAsync("a1", RoutingType.Anycast);
            await connection.CreateConsumerAsync("a1", RoutingType.Anycast);

            Assert.True(consumersAttached.Wait(Timeout));
            consumersAttached.Reset();

            host1.Dispose();

            var host2 = CreateOpenedContainerHost(endpoint, testHandler);

            Assert.True(consumersAttached.Wait(Timeout));

            await DisposeUtil.DisposeAll(connection, host2);
        }

        [Fact]
        public async Task Should_not_recreate_disposed_consumers()
        {
            var endpoint = GetUniqueEndpoint();
            var consumerAttached = new ManualResetEvent(false);
            var connectionRecovered = new ManualResetEvent(false);
            var testHandler = new TestHandler(@event =>
            {
                switch (@event.Id)
                {
                    case EventId.LinkRemoteOpen when @event.Context is Attach attach && !attach.Role:
                        consumerAttached.Set();
                        break;
                }
            });

            var host1 = CreateOpenedContainerHost(endpoint, testHandler);

            var connection = await CreateConnection(endpoint);
            connection.ConnectionRecovered += (_, _) => connectionRecovered.Set();
            var consumer = await connection.CreateConsumerAsync("a1", RoutingType.Anycast);

            Assert.True(consumerAttached.WaitOne(Timeout));
            await consumer.DisposeAsync();

            consumerAttached.Reset();

            await DisposeHostAndWaitUntilConnectionNotified(host1, connection);

            var host2 = CreateOpenedContainerHost(endpoint, testHandler);

            Assert.True(connectionRecovered.WaitOne(Timeout));

            Assert.False(consumerAttached.WaitOne(ShortTimeout));

            await DisposeUtil.DisposeAll(connection, host2);
        }

        private async Task<(IConsumer consumer, MessageSource messageSource, TestContainerHost host, IConnection connection)> CreateReattachedConsumer()
        {
            var endpoint = GetUniqueEndpoint();
            var consumerAttached = new ManualResetEvent(false);

            var testHandler = new TestHandler(@event =>
            {
                switch (@event.Id)
                {
                    case EventId.LinkLocalOpen when @event.Context is Attach attach && !attach.Role:
                        consumerAttached.Set();
                        break;
                }
            });

            var host1 = CreateOpenedContainerHost(endpoint, testHandler);

            var connection = await CreateConnection(endpoint);
            var consumer = await connection.CreateConsumerAsync("a1", RoutingType.Anycast);

            Assert.True(consumerAttached.WaitOne(Timeout));
            host1.Dispose();
            consumerAttached.Reset();

            var host2 = CreateContainerHost(endpoint, testHandler);
            var messageSource = host2.CreateMessageSource("a1");
            host2.Open();

            // wait until sender link is reattached
            Assert.True(consumerAttached.WaitOne(Timeout));

            return (consumer, messageSource, host2, connection);
        }
    }
}