using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ActiveMQ.Artemis.Client.Extensions.HealthChecks
{
    /// <summary>
    /// Service for managing ActiveMQ Artemis health checks.
    /// </summary>
    internal class ArtemisHealthCheckService : IHealthCheck
    {
        private readonly object _sync = new();
        private readonly HashSet<IConsumer> _unavailableConsumers = new();
        private bool _connectionOpen;
        private string _connectionDescription = "Connection not established";

        /// <summary>
        /// Marks the connection as open and updates the description accordingly.
        /// </summary>
        public void ConnectionOpen()
        {
            lock (_sync)
            {
                _connectionOpen = true;
                _connectionDescription = "Connection is open";
            }
        }

        /// <summary>
        /// Marks the connection as recovered and updates the description accordingly.
        /// </summary>
        public void ConnectionRecovered(object sender, ConnectionRecoveredEventArgs e, string description = "Connection recovered")
        {
            lock (_sync)
            {
                _connectionOpen = true;
                _connectionDescription = description;
            }
        }

        /// <summary>
        /// Marks the connection as closed and updates the description accordingly.
        /// </summary>
        public void ConnectionRecoveryError(object sender, ConnectionRecoveryErrorEventArgs e, string description = "Connection recovery failed")
        {
            lock (_sync)
            {
                _connectionOpen = false;
                _connectionDescription = description;
            }
        }

        /// <summary>
        /// Marks the connection as closed and updates the description accordingly.
        /// </summary>
        public void ConnectionClosed(object sender, ConnectionClosedEventArgs e, string description = "Connection closed")
        {
            lock (_sync)
            {
                _connectionOpen = false;
                _connectionDescription = description;
            }
        }

        public void ConsumerStateChanged(object sender, ConsumerLifecycleEventArgs e)
        {
            lock (_sync)
            {
                switch (e.State)
                {
                    case ConsumerLifecycleState.Unavailable:
                    case ConsumerLifecycleState.RecoveryFailed:
                        _unavailableConsumers.Add(e.Consumer);
                        break;
                    case ConsumerLifecycleState.Recovered:
                    case ConsumerLifecycleState.Disposed:
                        _unavailableConsumers.Remove(e.Consumer);
                        break;
                }
            }
        }

        /// <summary>
        /// Checks the health of the ActiveMQ connection.
        /// </summary>
        public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        {
            lock (_sync)
            {
                if (!_connectionOpen)
                {
                    return Task.FromResult(new HealthCheckResult(HealthStatus.Unhealthy, description: _connectionDescription));
                }

                if (_unavailableConsumers.Count > 0)
                {
                    return Task.FromResult(new HealthCheckResult(HealthStatus.Unhealthy, description: "One or more consumers are unavailable"));
                }

                return Task.FromResult(new HealthCheckResult(HealthStatus.Healthy, description: _connectionDescription));
            }
        }
    }
}
