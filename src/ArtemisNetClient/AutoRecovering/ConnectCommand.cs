namespace ActiveMQ.Artemis.Client.AutoRecovering
{
    internal class ConnectCommand
    {
        private ConnectCommand(AutoRecoveringConsumer consumer = null)
        {
            Consumer = consumer;
        }

        public AutoRecoveringConsumer Consumer { get; }
        public static readonly ConnectCommand Instance = new();
        public static ConnectCommand ForConsumer(AutoRecoveringConsumer consumer) => new(consumer);
    }
}