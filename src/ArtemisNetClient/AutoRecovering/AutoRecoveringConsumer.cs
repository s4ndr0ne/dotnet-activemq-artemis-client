using System;
using System.Threading;
using System.Threading.Tasks;
using ActiveMQ.Artemis.Client.Exceptions;
using ActiveMQ.Artemis.Client.Transactions;
using Amqp;
using Microsoft.Extensions.Logging;
using Nito.AsyncEx;

namespace ActiveMQ.Artemis.Client.AutoRecovering
{
    internal class AutoRecoveringConsumer : IConsumer, IRecoverable
    {
        private readonly ILogger<AutoRecoveringConsumer> _logger;
        private readonly ConsumerConfiguration _configuration;
        private readonly AsyncManualResetEvent _manualResetEvent = new(true);
        private volatile bool _closed;
        private volatile bool _disposed;
        private int _consumerUnavailable;
        private volatile Exception _failureCause;
        private volatile IConsumer _consumer;
        private volatile Exception _lastUnavailableCause;

        public AutoRecoveringConsumer(ILoggerFactory loggerFactory, ConsumerConfiguration configuration)
        {
            _logger = loggerFactory.CreateLogger<AutoRecoveringConsumer>();
            _configuration = configuration;
        }

        public async ValueTask<Message> ReceiveAsync(CancellationToken cancellationToken = default)
        {
            while (true)
            {
                CheckState();

                var consumer = _consumer;
                try
                {
                    return await consumer.ReceiveAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (ConsumerClosedException exception)
                {
                    if (!ReferenceEquals(consumer, _consumer))
                    {
                        continue;
                    }

                    NotifyConsumerUnavailable(exception);
                    CheckState();

                    Log.RetryingReceiveAsync(_logger);

                    Suspend();
                    ConsumerRecoveryRequested?.Invoke(this);
                    await _manualResetEvent.WaitAsync(cancellationToken).ConfigureAwait(false);
                }
            }
        }

        public ValueTask AcceptAsync(Message message, Transaction transaction, CancellationToken cancellationToken = default)
        {
            CheckState();

            return _consumer.AcceptAsync(message, transaction, cancellationToken);
        }

        public void Modify(Message message, bool deliveryFailed, bool undeliverableHere)
        {
            CheckState();

            _consumer.Modify(message, deliveryFailed, undeliverableHere);
        }

        public void Reject(Message message)
        {
            CheckState();

            _consumer.Reject(message);
        }

        private void CheckState()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(AutoRecoveringConsumer));
            }

            if (_closed)
            {
                if (_failureCause != null)
                {
                    throw new ConsumerClosedException("The Consumer was closed due to an unrecoverable error.", _failureCause);
                }
                else
                {
                    throw new ConsumerClosedException();
                }
            }
        }

        public void Suspend()
        {
            var wasSuspended = IsSuspended();
            _manualResetEvent.Reset();

            if (!wasSuspended)
            {
                Log.ConsumerSuspended(_logger);
            }
        }

        public void Resume()
        {
            var wasSuspended = IsSuspended();
            _manualResetEvent.Set();

            if (wasSuspended)
            {
                Log.ConsumerResumed(_logger);
            }
        }

        private bool IsSuspended()
        {
            return !_manualResetEvent.IsSet;
        }

        public async Task RecoverAsync(IConnection connection, CancellationToken cancellationToken)
        {
            var oldConsumer = _consumer;
            // Detach first so late close events from the old link cannot emit a
            // duplicate Unavailable after we consume the pending signal below.
            // _consumer is swapped only after successful creation, so a failure
            // keeps pointing at the old (closed) instance and ReceiveAsync keeps
            // throwing ConsumerClosedException instead of NRE.
            DetachLifecycleNotifications(oldConsumer);
            // Consume the pending unavailable signal so that failures arriving
            // during recovery are preserved (flag goes 0 -> 1 again) instead of lost.
            var hadPendingSignal = Interlocked.Exchange(ref _consumerUnavailable, 0) != 0;

            IConsumer newConsumer;
            try
            {
                newConsumer = await connection.CreateConsumerAsync(_configuration, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception createException)
            {
                if (hadPendingSignal)
                {
                    // This outage was already reported (E1 emitted before recovery).
                    // Restore the flag without emitting a duplicate Unavailable.
                    Interlocked.Exchange(ref _consumerUnavailable, 1);
                }
                else
                {
                    // Force-recovery path with no prior signal: report the failure.
                    NotifyConsumerUnavailable(createException);
                }

                throw;
            }

            _consumer = newConsumer;
            await DisposeUnderlyingConsumerSafe(oldConsumer).ConfigureAwait(false);
            AttachLifecycleNotifications(newConsumer);
            Log.ConsumerRecovered(_logger);

            if (IsConsumerUnavailable)
            {
                // A new failure arrived while recreating (e.g. duplicate close signal
                // on the old consumer or immediate close of the new link).
                // Do not emit Recovered: caller must schedule another retry instead of resuming.
                return;
            }

            if (oldConsumer != null)
            {
                ConsumerStateChanged?.Invoke(new ConsumerLifecycleEventArgs(this, ConsumerLifecycleState.Recovered));
            }
        }

        public async Task TerminateAsync(Exception exception)
        {
            _closed = true;
            _failureCause = exception;
            _manualResetEvent.Set();
            Log.ConsumerTerminated(_logger, exception);
            DetachLifecycleNotifications(_consumer);
            await DisposeUnderlyingConsumerSafe(_consumer).ConfigureAwait(false);
            ConsumerStateChanged?.Invoke(new ConsumerLifecycleEventArgs(this, ConsumerLifecycleState.RecoveryFailed, exception));
            Closed?.Invoke(this);
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                DetachLifecycleNotifications(_consumer);
                await DisposeUnderlyingConsumer(_consumer).ConfigureAwait(false);
            }
            finally
            {
                _disposed = true;
                _manualResetEvent.Set();
                ConsumerStateChanged?.Invoke(new ConsumerLifecycleEventArgs(this, ConsumerLifecycleState.Disposed));
                Closed?.Invoke(this);
            }
        }

        internal bool IsConsumerUnavailable => Volatile.Read(ref _consumerUnavailable) != 0;

        internal bool IsTerminatedOrDisposed => _closed || _disposed;

        private void AttachLifecycleNotifications(IConsumer consumer)
        {
            if (consumer is Consumer concreteConsumer)
            {
                concreteConsumer.LifecycleChanged += OnUnderlyingConsumerLifecycleChanged;
            }
        }

        private void DetachLifecycleNotifications(IConsumer consumer)
        {
            if (consumer is Consumer concreteConsumer)
            {
                concreteConsumer.LifecycleChanged -= OnUnderlyingConsumerLifecycleChanged;
            }
        }

        private void OnUnderlyingConsumerLifecycleChanged(object sender, ConsumerLifecycleEventArgs args)
        {
            if (ReferenceEquals(sender, _consumer) && args.State == ConsumerLifecycleState.Unavailable)
            {
                NotifyConsumerUnavailable(args.Exception);
                Suspend();
                ConsumerRecoveryRequested?.Invoke(this);
            }
        }

        internal void NotifyConsumerUnavailable(Exception exception)
        {
            // The same close is reported twice: once via ReceiveAsync's channel
            // completion and once via the underlying link lifecycle event. Both carry
            // the same exception instance, which may arrive late (after recovery
            // already consumed the flag). Dedup by reference so a late duplicate for
            // the same close never emits a second Unavailable.
            if (exception != null && ReferenceEquals(exception, Volatile.Read(ref _lastUnavailableCause)))
            {
                return;
            }

            if (Interlocked.Exchange(ref _consumerUnavailable, 1) == 0)
            {
                if (exception != null)
                {
                    Volatile.Write(ref _lastUnavailableCause, exception);
                }

                ConsumerStateChanged?.Invoke(new ConsumerLifecycleEventArgs(this, ConsumerLifecycleState.Unavailable, exception));
            }
        }

        private async Task DisposeUnderlyingConsumerSafe(IConsumer consumer)
        {
            try
            {
                await DisposeUnderlyingConsumer(consumer).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // ignored
            }
        }

        private static async ValueTask DisposeUnderlyingConsumer(IConsumer consumer)
        {
            if (consumer != null)
            {
                await consumer.DisposeAsync().ConfigureAwait(false);
            }
        }

        public event Closed Closed;

        public event RecoveryRequested RecoveryRequested;

        public event Action<AutoRecoveringConsumer> ConsumerRecoveryRequested;

        public event Action<ConsumerLifecycleEventArgs> ConsumerStateChanged;

        private static class Log
        {
            private static readonly Action<ILogger, Exception> _retryingConsumeAsync = LoggerMessage.Define(
                LogLevel.Warning,
                0,
                "Retrying receive after Consumer reestablished.");
            
            private static readonly Action<ILogger, Exception> _consumerRecovered = LoggerMessage.Define(
                LogLevel.Information,
                0,
                "Consumer recovered.");
            
            private static readonly Action<ILogger, Exception> _consumerSuspended = LoggerMessage.Define(
                LogLevel.Warning,
                0,
                "Consumer suspended.");
            
            private static readonly Action<ILogger, Exception> _consumerResumed = LoggerMessage.Define(
                LogLevel.Information,
                0,
                "Consumer resumed.");
            
            private static readonly Action<ILogger, Exception> _consumerTerminated = LoggerMessage.Define(
                LogLevel.Error,
                0,
                "Consumer terminated.");
            
            public static void RetryingReceiveAsync(ILogger logger)
            {
                if (logger.IsEnabled(LogLevel.Warning))
                {
                    _retryingConsumeAsync(logger, null);    
                }
            }
            
            public static void ConsumerRecovered(ILogger logger)
            {
                if (logger.IsEnabled(LogLevel.Information))
                {
                    _consumerRecovered(logger, null);
                }
            }
            
            public static void ConsumerSuspended(ILogger logger)
            {
                if (logger.IsEnabled(LogLevel.Warning))
                {
                    _consumerSuspended(logger, null);
                }
            }
            
            public static void ConsumerResumed(ILogger logger)
            {
                if (logger.IsEnabled(LogLevel.Information))
                {
                    _consumerResumed(logger, null);
                }
            }
            
            public static void ConsumerTerminated(ILogger logger, Exception exception)
            {
                if (logger.IsEnabled(LogLevel.Error))
                {
                    _consumerTerminated(logger, exception);
                }
            }
        }
    }
}