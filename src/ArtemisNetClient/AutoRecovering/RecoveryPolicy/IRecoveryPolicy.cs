using System;

namespace ActiveMQ.Artemis.Client.AutoRecovering.RecoveryPolicy
{
    /// <summary>
    /// Controls delays and limits for automatic recovery.
    /// Applies to both connection recovery and per-consumer recovery
    /// (e.g. waiting for a deleted queue to be recreated).
    /// Use <c>RetryCount = int.MaxValue</c> (default) to retry indefinitely.
    /// When the retry limit is exceeded the consumer is terminated and
    /// <see cref="IConsumer"/> operations throw
    /// <see cref="Exceptions.ConsumerClosedException"/>; health checks observe
    /// <see cref="ConsumerLifecycleState.RecoveryFailed"/>.
    /// </summary>
    public interface IRecoveryPolicy
    {
        int RetryCount { get; }
        TimeSpan GetDelay(int attempt);
    }
}