using System;
using System.Threading;
using System.Threading.Tasks;
using ActiveMQ.Artemis.Client.AutoRecovering.RecoveryPolicy;
using ActiveMQ.Artemis.Client.Exceptions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ActiveMQ.Artemis.Client.Extensions.DependencyInjection
{
    internal class ActiveMqConsumer
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly ContextualReceiveObservable _receiveObservable;
        private readonly Func<CancellationToken, Task<IConsumer>> _consumerFactory;
        private readonly Func<Message, IConsumer, IServiceProvider, CancellationToken, Task> _handler;
        private readonly IRecoveryPolicy _recoveryPolicy;
        private readonly object _consumerLock = new();
        private Task _task;
        private CancellationTokenSource _cts;
        private readonly ILogger<ActiveMqConsumer> _logger;
        private volatile IConsumer _consumer;

        public ActiveMqConsumer(IServiceProvider serviceProvider,
            ContextualReceiveObservable receiveObservable,
            Func<CancellationToken, Task<IConsumer>> consumerFactory,
            Func<Message, IConsumer, IServiceProvider, CancellationToken, Task> handler)
            : this(serviceProvider, receiveObservable, consumerFactory, handler, recoveryPolicy: null)
        {
        }

        public ActiveMqConsumer(IServiceProvider serviceProvider,
            ContextualReceiveObservable receiveObservable,
            Func<CancellationToken, Task<IConsumer>> consumerFactory,
            Func<Message, IConsumer, IServiceProvider, CancellationToken, Task> handler,
            IRecoveryPolicy recoveryPolicy)
        {
            _serviceProvider = serviceProvider;
            _receiveObservable = receiveObservable;
            _logger = serviceProvider.GetService<ILogger<ActiveMqConsumer>>();
            _consumerFactory = consumerFactory;
            _handler = handler;
            _recoveryPolicy = recoveryPolicy ?? RecoveryPolicyFactory.Default();
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var token = _cts.Token;
            var initialConsumer = await _consumerFactory(cancellationToken).ConfigureAwait(false);
            lock (_consumerLock)
            {
                _consumer = initialConsumer;
            }
            _task = Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    IConsumer snapshot;
                    lock (_consumerLock)
                    {
                        snapshot = _consumer;
                    }

                    if (snapshot == null)
                    {
                        return;
                    }

                    try
                    {
                        var msg = await snapshot.ReceiveAsync(token).ConfigureAwait(false);
                        _receiveObservable.PreReceive(msg);
                        await _handler(msg, snapshot, _serviceProvider, token).ConfigureAwait(false);
                        _receiveObservable.PostReceive(msg);
                    }
                    catch (OperationCanceledException)
                    {
                    }
                    catch (Exception exception)
                    {
                        _logger.LogError(exception, "Error while receiving message.");

                        if (exception is ConsumerClosedException)
                        {
                            await RecreateConsumerAsync(token).ConfigureAwait(false);
                        }
                    }
                }
            }, token);
        }

        private async Task RecreateConsumerAsync(CancellationToken cancellationToken)
        {
            // Hosted consumers retry indefinitely with backoff: stopping the background
            // loop silently would leave the host running without consuming. The attempt
            // counter is clamped to RetryCount so GetDelay stays bounded for NoLimit
            // policies and converges to the max delay for limited ones.
            var attempt = 0;
            var maxAttempt = Math.Max(1, _recoveryPolicy.RetryCount);

            while (!cancellationToken.IsCancellationRequested)
            {
                IConsumer newConsumer;
                try
                {
                    attempt = attempt >= maxAttempt ? maxAttempt : attempt + 1;
                    var delay = _recoveryPolicy.GetDelay(attempt);
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                    newConsumer = await _consumerFactory(cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception exception)
                {
                    _logger.LogError(exception, "Failed to recreate consumer; retrying.");
                    continue;
                }

                if (cancellationToken.IsCancellationRequested)
                {
                    await DisposeConsumerSafe(newConsumer).ConfigureAwait(false);
                    return;
                }

                IConsumer oldConsumer;
                lock (_consumerLock)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        oldConsumer = null;
                    }
                    else
                    {
                        oldConsumer = _consumer;
                        _consumer = newConsumer;
                        newConsumer = null;
                    }
                }

                // StopAsync won the race after we created the replacement:
                // dispose it instead of leaking it.
                if (newConsumer != null)
                {
                    await DisposeConsumerSafe(newConsumer).ConfigureAwait(false);
                    return;
                }

                if (oldConsumer != null)
                {
                    await DisposeConsumerSafe(oldConsumer).ConfigureAwait(false);
                }

                return;
            }
        }

        public async Task StopAsync()
        {
            try
            {
                var cts = _cts;
                var task = _task;
                if (cts == null || task == null)
                {
                    return;
                }

                cts.Cancel();
                await task.ConfigureAwait(false);
                cts.Dispose();

                IConsumer snapshot;
                lock (_consumerLock)
                {
                    snapshot = _consumer;
                    _consumer = null;
                    _cts = null;
                    _task = null;
                }

                if (snapshot != null)
                {
                    await DisposeConsumerSafe(snapshot).ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Exception occured during consumer stopping.");
            }
        }

        private async Task DisposeConsumerSafe(IConsumer consumer)
        {
            if (consumer == null)
            {
                return;
            }

            try
            {
                await consumer.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "Failed to dispose consumer.");
            }
        }
    }
}
