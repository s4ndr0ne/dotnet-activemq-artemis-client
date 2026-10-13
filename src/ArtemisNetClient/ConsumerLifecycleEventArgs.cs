using System;

namespace ActiveMQ.Artemis.Client
{
    /// <summary>
    /// Describes a consumer's availability during its lifecycle.
    /// </summary>
    public enum ConsumerLifecycleState
    {
        /// <summary>The consumer closed and is not currently available.</summary>
        Unavailable,
        /// <summary>The consumer has been successfully recreated.</summary>
        Recovered,
        /// <summary>The consumer could not be recreated.</summary>
        RecoveryFailed,
        /// <summary>The consumer was explicitly disposed.</summary>
        Disposed
    }

    /// <summary>Provides details about a consumer lifecycle state change.</summary>
    public class ConsumerLifecycleEventArgs : EventArgs
    {
        /// <summary>Creates event data for a consumer lifecycle state change.</summary>
        public ConsumerLifecycleEventArgs(IConsumer consumer, ConsumerLifecycleState state, Exception exception = null)
        {
            Consumer = consumer;
            State = state;
            Exception = exception;
        }

        /// <summary>Gets the consumer whose state changed.</summary>
        public IConsumer Consumer { get; }

        /// <summary>Gets the new lifecycle state.</summary>
        public ConsumerLifecycleState State { get; }

        /// <summary>Gets the exception associated with an unavailable or failed consumer, if any.</summary>
        public Exception Exception { get; }
    }

    /// <summary>Exposes consumer lifecycle notifications from a connection.</summary>
    public interface IConsumerLifecycleEvents
    {
        /// <summary>Raised when a consumer becomes unavailable, is recovered, fails recovery, or is disposed.</summary>
        event EventHandler<ConsumerLifecycleEventArgs> ConsumerStateChanged;
    }
}
