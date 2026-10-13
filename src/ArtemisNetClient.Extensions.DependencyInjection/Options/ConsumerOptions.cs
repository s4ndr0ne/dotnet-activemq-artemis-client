namespace ActiveMQ.Artemis.Client.Extensions.DependencyInjection
{
    /// <summary>
    /// Provides programmatic configuration for <see cref="IConsumer"/>. 
    /// </summary>
    public class ConsumerOptions
    {
        /// <summary>
        /// Specifies the number of concurrent consumers to create. Default is 1.
        /// </summary>
        public int ConcurrentConsumers { get; set; } = 1;
        
        /// <summary>
        /// The link credit to issue. The credit controls how many messages the peer can send.
        /// </summary>
        public int Credit { get; set; } = 200;

        public string FilterExpression { get; set; }

        public bool NoLocalFilter { get; set; }

        /// <summary>
        /// Recovery policy used by the hosted consumer to recreate its
        /// <see cref="IConsumer"/> after <see cref="Exceptions.ConsumerClosedException"/>.
        /// Retries indefinitely with backoff; attempt is clamped to
        /// <see cref="AutoRecovering.RecoveryPolicy.IRecoveryPolicy.RetryCount"/> so the delay stays bounded.
        /// Default is <see cref="AutoRecovering.RecoveryPolicy.RecoveryPolicyFactory.Default"/>.
        /// </summary>
        public AutoRecovering.RecoveryPolicy.IRecoveryPolicy RecoveryPolicy { get; set; }
    }
}