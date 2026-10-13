using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using ActiveMQ.Artemis.Client.AutoRecovering.RecoveryPolicy;
using ActiveMQ.Artemis.Client.Builders;
using ActiveMQ.Artemis.Client.Exceptions;
using ActiveMQ.Artemis.Client.InternalUtilities;
using ActiveMQ.Artemis.Client.MessageIdPolicy;
using Amqp;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;

namespace ActiveMQ.Artemis.Client.AutoRecovering
{
    internal class AutoRecoveringConnection : IConnection, IConsumerLifecycleEvents
    {
        private IConnection _connection;
        private readonly ILoggerFactory _loggerFactory;
        private readonly Func<IMessageIdPolicy> _messageIdPolicyFactory;
        private readonly Func<string> _clientIdFactory;
        private readonly SslSettings _sslSettings;
        private readonly TcpSettings _tcpSettings;
        private readonly ILogger<AutoRecoveringConnection> _logger;
        private readonly Endpoint[] _endpoints;
        private readonly ChannelReader<ConnectCommand> _reader;
        private readonly ChannelWriter<ConnectCommand> _writer;
        private readonly ConcurrentHashSet<IRecoverable> _recoverables = new();
        private readonly ConcurrentDictionary<AutoRecoveringConsumer, int> _consumerRecoveryAttempts = new();
        private readonly ConcurrentDictionary<AutoRecoveringConsumer, byte> _scheduledConsumerRecoveries = new();
        private readonly ConcurrentDictionary<AutoRecoveringConsumer, byte> _consumerRecoveryInFlight = new();
        private readonly CancellationTokenSource _recoveryCancellationToken = new();
        private readonly IRecoveryPolicy _recoveryPolicy;
        private readonly AsyncRetryPolicy<IConnection> _connectionRetryPolicy;
        private readonly Task _recoveryLoopTask;

        public AutoRecoveringConnection(ILoggerFactory loggerFactory,
            IEnumerable<Endpoint> endpoints,
            IRecoveryPolicy recoveryPolicy,
            Func<IMessageIdPolicy> messageIdPolicyFactory,
            Func<string> clientIdFactory,
            SslSettings sslSettings,
            TcpSettings tcpSettings)
        {
            _logger = loggerFactory.CreateLogger<AutoRecoveringConnection>();
            _loggerFactory = loggerFactory;
            _messageIdPolicyFactory = messageIdPolicyFactory;
            _clientIdFactory = clientIdFactory;
            _sslSettings = sslSettings;
            _tcpSettings = tcpSettings;
            _endpoints = endpoints.ToArray();
            _recoveryPolicy = recoveryPolicy;

            var channel = Channel.CreateUnbounded<ConnectCommand>();
            _reader = channel.Reader;
            _writer = channel.Writer;

            _connectionRetryPolicy = CreateConnectionRetryPolicy(recoveryPolicy);

            _recoveryLoopTask = StartRecoveryLoop();
        }

        public Endpoint Endpoint => _connection.Endpoint;

        // TODO: Probably should return false only when connection was explicitly closed.
        public bool IsOpened => _connection != null && _connection.IsOpened;

        private AsyncRetryPolicy<IConnection> CreateConnectionRetryPolicy(IRecoveryPolicy recoveryPolicy)
        {
            return Policy<IConnection>
                   .Handle<Exception>(exception => exception switch
                   {
                       CreateConnectionException { ErrorCode: ErrorCode.UnauthorizedAccess } => false,
                       _ => true
                   })
                   .WaitAndRetryAsync(recoveryPolicy.RetryCount, (retryAttempt, context) =>
                   {
                       context.SetRetryCount(retryAttempt);
                       return recoveryPolicy.GetDelay(retryAttempt);
                   }, (result, _, context) =>
                   {
                       var retryCount = context.GetRetryCount();
                       var endpoint = context.GetEndpoint();
                       if (result.Exception != null)
                       {
                           Log.FailedToEstablishConnection(_logger, endpoint, retryCount, result.Exception);
                       }
                   });
        }

        private Task StartRecoveryLoop()
        {
            return Task.Run(async () =>
            {
                try
                {
                    while (true)
                    {
                        var command = await _reader.ReadAsync(_recoveryCancellationToken.Token).ConfigureAwait(false);

                        if (command.Consumer != null)
                        {
                            if (IsOpened)
                            {
                                await TryRecoverConsumerAsync(command.Consumer).ConfigureAwait(false);
                            }

                            continue;
                        }

                        if (!IsOpened)
                        {
                            await DisposeInnerConnection().ConfigureAwait(false);

                            foreach (var recoverable in _recoverables.Values)
                            {
                                recoverable.Suspend();
                            }

                            _connection = await CreateConnection(_recoveryCancellationToken.Token).ConfigureAwait(false);
                            _connection.ConnectionClosed += OnConnectionClosed;

                            foreach (var recoverable in _recoverables.Values)
                            {
                                if (recoverable is AutoRecoveringConsumer consumer)
                                {
                                    await TryRecoverConsumerAsync(consumer, forceRecovery: true).ConfigureAwait(false);
                                    continue;
                                }

                                try
                                {
                                    await recoverable.RecoverAsync(_connection, _recoveryCancellationToken.Token).ConfigureAwait(false);
                                    recoverable.Resume();
                                }
                                catch (Exception e)
                                {
                                    await recoverable.TerminateAsync(e).ConfigureAwait(false);
                                }
                            }

                            Log.ConnectionRecovered(_logger);

                            ConnectionRecovered?.Invoke(this, new ConnectionRecoveredEventArgs(_connection.Endpoint));
                        }
                        else
                        {
                            foreach (var recoverable in _recoverables.Values)
                            {
                                if (recoverable is AutoRecoveringConsumer consumer && consumer.IsConsumerUnavailable)
                                {
                                    await TryRecoverConsumerAsync(consumer).ConfigureAwait(false);
                                }
                                else
                                {
                                    recoverable.Resume();
                                }
                            }
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // expected when recovery cancellation token is set.
                }
                catch (Exception e)
                {
                    Log.MainRecoveryLoopException(_logger, e);

                    foreach (var recoverable in _recoverables.Values)
                    {
                        await recoverable.TerminateAsync(e).ConfigureAwait(false);
                    }

                    ConnectionRecoveryError?.Invoke(this, new ConnectionRecoveryErrorEventArgs(e));
                }
            });
        }

        private async Task TryRecoverConsumerAsync(AutoRecoveringConsumer consumer, bool forceRecovery = false)
        {
            if (consumer.IsTerminatedOrDisposed)
            {
                CleanupConsumerState(consumer);
                return;
            }

            // Serialize recovery per consumer: the recovery loop is single-threaded, but
            // ReceiveAsync failures and underlying-link lifecycle events can enqueue
            // duplicate commands for the same close. Coalesce them instead of running
            // overlapping RecoverAsync calls.
            if (!_consumerRecoveryInFlight.TryAdd(consumer, byte.MinValue))
            {
                return;
            }

            try
            {
                if (!forceRecovery && !consumer.IsConsumerUnavailable)
                {
                    consumer.Resume();
                    return;
                }

                try
                {
                    await consumer.RecoverAsync(_connection, _recoveryCancellationToken.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (_recoveryCancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception exception)
                {
                    consumer.NotifyConsumerUnavailable(exception);
                    _logger.LogWarning(exception, "Failed to recreate consumer; retrying with backoff.");
                    ScheduleConsumerRecovery(consumer, exception);
                    return;
                }

                if (consumer.IsTerminatedOrDisposed)
                {
                    CleanupConsumerState(consumer);
                    return;
                }

                if (consumer.IsConsumerUnavailable)
                {
                    // A new failure arrived while recreating: schedule a retry instead of resuming.
                    ScheduleConsumerRecovery(consumer, new ConsumerClosedException("Consumer became unavailable during recovery.", (string)null));
                    return;
                }

                consumer.Resume();
                _consumerRecoveryAttempts.TryRemove(consumer, out _);
            }
            finally
            {
                _consumerRecoveryInFlight.TryRemove(consumer, out _);
            }
        }

        private void ScheduleConsumerRecovery(AutoRecoveringConsumer consumer, Exception failure)
        {
            if (consumer.IsTerminatedOrDisposed || !consumer.IsConsumerUnavailable || !_scheduledConsumerRecoveries.TryAdd(consumer, byte.MinValue))
            {
                return;
            }

            var attempt = _consumerRecoveryAttempts.AddOrUpdate(consumer, 1, (_, previous) => previous >= int.MaxValue ? int.MaxValue : previous + 1);
            if (attempt > _recoveryPolicy.RetryCount)
            {
                // Recovery policy gave up: terminate the consumer so ReceiveAsync throws
                // ConsumerClosedException instead of waiting forever. Health checks observe
                // RecoveryFailed via ConsumerStateChanged.
                _scheduledConsumerRecoveries.TryRemove(consumer, out _);
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await consumer.TerminateAsync(failure).ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        _logger.LogError(exception, "Failed to terminate consumer after recovery policy gave up.");
                    }
                    finally
                    {
                        CleanupConsumerState(consumer);
                    }
                });
                return;
            }

            var delay = _recoveryPolicy.GetDelay(attempt);
            var maxDelay = TimeSpan.FromMilliseconds(int.MaxValue);
            if (delay > maxDelay)
            {
                delay = maxDelay;
            }

            _ = Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(delay, _recoveryCancellationToken.Token).ConfigureAwait(false);
                    _scheduledConsumerRecoveries.TryRemove(consumer, out _);

                    if (!consumer.IsTerminatedOrDisposed && consumer.IsConsumerUnavailable)
                    {
                        _writer.TryWrite(ConnectCommand.ForConsumer(consumer));
                    }
                }
                catch (OperationCanceledException)
                {
                    _scheduledConsumerRecoveries.TryRemove(consumer, out _);
                }
                catch (Exception exception)
                {
                    _scheduledConsumerRecoveries.TryRemove(consumer, out _);
                    _logger.LogError(exception, "Consumer recovery retry could not be scheduled.");
                }
            });
        }

        private void OnConnectionClosed(object sender, ConnectionClosedEventArgs args)
        {
            if (args.ClosedByPeer)
            {
                Log.ConnectionClosedByPeer(_logger, args.Error);
                _writer.TryWrite(ConnectCommand.Instance);
            }
            else
            {
                Log.ConnectionClosed(_logger);
            }

            ConnectionClosed?.Invoke(sender, args);
        }

        public async Task InitAsync(CancellationToken cancellationToken)
        {
            _connection = await CreateConnection(cancellationToken).ConfigureAwait(false);
            _connection.ConnectionClosed += OnConnectionClosed;
        }

        private Task<IConnection> CreateConnection(CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetRetryCount(0);
            return _connectionRetryPolicy.ExecuteAsync(async (context, ct) =>
            {
                int retryCount = context.GetRetryCount();
                var endpoint = GetNextEndpoint(retryCount);
                context.SetEndpoint(endpoint);
                var connectionBuilder = new ConnectionBuilder(_loggerFactory, _messageIdPolicyFactory, _clientIdFactory, _sslSettings, _tcpSettings);
                
                Log.TryingToEstablishedConnection(_logger, endpoint, retryCount + 1);
                var connection = await connectionBuilder.CreateAsync(endpoint, ct).ConfigureAwait(false);
                Log.ConnectionEstablished(_logger, endpoint, retryCount + 1);
                
                return connection;
            }, ctx, cancellationToken);
        }

        public async Task<ITopologyManager> CreateTopologyManagerAsync(CancellationToken cancellationToken = default)
        {
            var configuration = new RequestReplyClientConfiguration
            {
                Address = "activemq.management"
            };
            var rpcClient = await CreateRequestReplyClientAsync(configuration, cancellationToken).ConfigureAwait(false);
            return new TopologyManager(configuration.Address, rpcClient);
        }

        public async Task<IConsumer> CreateConsumerAsync(ConsumerConfiguration configuration, CancellationToken cancellationToken)
        {
            var autoRecoveringConsumer = new AutoRecoveringConsumer(_loggerFactory, configuration);
            await PrepareRecoverable(autoRecoveringConsumer, cancellationToken).ConfigureAwait(false);
            return autoRecoveringConsumer;
        }

        public async Task<IProducer> CreateProducerAsync(ProducerConfiguration configuration, CancellationToken cancellationToken)
        {
            var autoRecoveringProducer = new AutoRecoveringProducer(_loggerFactory, configuration);
            await PrepareRecoverable(autoRecoveringProducer, cancellationToken).ConfigureAwait(false);
            return autoRecoveringProducer;
        }

        public async Task<IAnonymousProducer> CreateAnonymousProducerAsync(AnonymousProducerConfiguration configuration, CancellationToken cancellationToken = default)
        {
            var autoRecoveringAnonymousProducer = new AutoRecoveringAnonymousProducer(_loggerFactory, configuration);
            await PrepareRecoverable(autoRecoveringAnonymousProducer, cancellationToken).ConfigureAwait(false);
            return autoRecoveringAnonymousProducer;
        }

        public async Task<IRequestReplyClient> CreateRequestReplyClientAsync(RequestReplyClientConfiguration configuration, CancellationToken cancellationToken = default)
        {
            var autoRecoveringRpcClient = new AutoRecoveringRequestReplyClient(_loggerFactory, configuration);
            await PrepareRecoverable(autoRecoveringRpcClient, cancellationToken).ConfigureAwait(false);
            return autoRecoveringRpcClient;
        }

        public event EventHandler<ConnectionClosedEventArgs> ConnectionClosed;
        public event EventHandler<ConnectionRecoveredEventArgs> ConnectionRecovered;
        public event EventHandler<ConnectionRecoveryErrorEventArgs> ConnectionRecoveryError;
        public event EventHandler<ConsumerLifecycleEventArgs> ConsumerStateChanged;

        private async Task PrepareRecoverable(IRecoverable recoverable, CancellationToken cancellationToken)
        {
            var consumer = recoverable as AutoRecoveringConsumer;
            if (consumer != null)
            {
                consumer.ConsumerStateChanged += OnConsumerStateChanged;
                consumer.ConsumerRecoveryRequested += OnConsumerRecoveryRequested;
            }

            try
            {
                await recoverable.RecoverAsync(_connection, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                if (consumer != null)
                {
                    consumer.ConsumerStateChanged -= OnConsumerStateChanged;
                    consumer.ConsumerRecoveryRequested -= OnConsumerRecoveryRequested;
                }

                throw;
            }

            _recoverables.Add(recoverable);
            recoverable.Closed += OnRecoverableClosed;
            recoverable.RecoveryRequested += OnRecoveryRequested;
        }

        private void OnRecoverableClosed(IRecoverable recoverable)
        {
            recoverable.Closed -= OnRecoverableClosed;
            recoverable.RecoveryRequested -= OnRecoveryRequested;
            if (recoverable is AutoRecoveringConsumer consumer)
            {
                consumer.ConsumerStateChanged -= OnConsumerStateChanged;
                consumer.ConsumerRecoveryRequested -= OnConsumerRecoveryRequested;
                CleanupConsumerState(consumer);
            }
            _recoverables.Remove(recoverable);
        }

        private void CleanupConsumerState(AutoRecoveringConsumer consumer)
        {
            _consumerRecoveryAttempts.TryRemove(consumer, out _);
            _scheduledConsumerRecoveries.TryRemove(consumer, out _);
            _consumerRecoveryInFlight.TryRemove(consumer, out _);
        }

        private void OnConsumerStateChanged(ConsumerLifecycleEventArgs args)
        {
            ConsumerStateChanged?.Invoke(this, args);
        }

        private void OnConsumerRecoveryRequested(AutoRecoveringConsumer consumer)
        {
            _writer.TryWrite(ConnectCommand.ForConsumer(consumer));
        }

        private void OnRecoveryRequested()
        {
            _writer.TryWrite(ConnectCommand.Instance);
        }

        public async ValueTask DisposeAsync()
        {
            _recoveryCancellationToken.Cancel();
            await _recoveryLoopTask.ConfigureAwait(false);
            await Task.WhenAll(_recoverables.Values.Select(async x => await x.DisposeAsync().ConfigureAwait(false))).ConfigureAwait(false);
            await DisposeInnerConnection().ConfigureAwait(false);
        }

        private async Task DisposeInnerConnection()
        {
            var connection = _connection;
            if (connection == null)
            {
                return;
            }

            connection.ConnectionClosed -= OnConnectionClosed;
            try
            {
                await connection.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
                // ignored
            }
        }

        private Endpoint GetNextEndpoint(int retryCount)
        {
            return _endpoints[retryCount % _endpoints.Length];
        }

        private static class Log
        {
            private static readonly Action<ILogger, string, int, Exception> _connectionEstablished = LoggerMessage.Define<string, int>(
                LogLevel.Information,
                0,
                "Connection to {endpoint} established. Attempt: {attempt}.");

            private static readonly Action<ILogger, string, int, Exception> _failedToEstablishConnection = LoggerMessage.Define<string, int>(
                LogLevel.Warning,
                0,
                "Failed to establish connection to {endpoint}. Attempt: {attempt}.");

            private static readonly Action<ILogger, Exception> _mainRecoveryLoopException = LoggerMessage.Define(
                LogLevel.Error,
                0,
                "Main recovery loop threw unexpected exception.");

            private static readonly Action<ILogger, string, Exception> _connectionClosedByPeer = LoggerMessage.Define<string>(
                LogLevel.Warning,
                0,
                "Connection closed due to {error}. Reconnect scheduled.");
            
            private static readonly Action<ILogger, Exception> _connectionClosed = LoggerMessage.Define(
                LogLevel.Information,
                0,
                "Connection closed. Reconnect won't be scheduled.");            

            private static readonly Action<ILogger, Exception> _connectionRecovered = LoggerMessage.Define(
                LogLevel.Information,
                0,
                "Connection recovered.");

            private static readonly Action<ILogger, string, int, Exception> _tryingToEstablishedConnection = LoggerMessage.Define<string, int>(
                LogLevel.Information,
                0,
                "Trying to establish connection to {endpoint}. Attempt: {attempt}.");

            public static void ConnectionEstablished(ILogger logger, Endpoint endpoint, int attempt)
            {
                if (logger.IsEnabled(LogLevel.Information))
                {
                    _connectionEstablished(logger, endpoint.ToString(), attempt, null);
                }
            }

            public static void FailedToEstablishConnection(ILogger logger, Endpoint endpoint, int attempt, Exception e)
            {
                if (logger.IsEnabled(LogLevel.Warning))
                {
                    _failedToEstablishConnection(logger, endpoint.ToString(), attempt, e);
                }
            }

            public static void MainRecoveryLoopException(ILogger logger, Exception e)
            {
                if (logger.IsEnabled(LogLevel.Error))
                {
                    _mainRecoveryLoopException(logger, e);
                }
            }

            public static void ConnectionClosedByPeer(ILogger logger, string error)
            {
                if (logger.IsEnabled(LogLevel.Warning))
                {
                    _connectionClosedByPeer(logger, error, null);
                }
            }
            
            public static void ConnectionClosed(ILogger logger)
            {
                if (logger.IsEnabled(LogLevel.Information))
                {
                    _connectionClosed(logger, null);
                }
            }

            public static void ConnectionRecovered(ILogger logger)
            {
                if (logger.IsEnabled(LogLevel.Information))
                {
                    _connectionRecovered(logger, null);
                }
            }
            
            public static void TryingToEstablishedConnection(ILogger logger, Endpoint endpoint, int attempt)
            {
                if (logger.IsEnabled(LogLevel.Information))
                {
                    _tryingToEstablishedConnection(logger, endpoint.ToString(), attempt, null);
                }
            }
        }
    }
}