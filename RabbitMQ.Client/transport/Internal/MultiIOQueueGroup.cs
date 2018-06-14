using System;
using System.Threading;
using RabbitMQ.Client.Transport.Internal;

namespace RabbitMQ.Client.transport.Internal
{
    public class MultiIOQueueGroup
    {
        private static readonly int DefaultIOQueueCount = Math.Max(Environment.ProcessorCount, 16);

        private int index;
        readonly IOQueue[] ioQueues;

        public MultiIOQueueGroup()
        {
            ioQueues = new IOQueue[DefaultIOQueueCount];
            for (int i = 0; i < DefaultIOQueueCount; i++)
            {
                ioQueues[i] = new IOQueue();
            }
        }

        public IOQueue GetIOQueue()
        {
            var k = Interlocked.Increment(ref index) % 16;
            return ioQueues[k];
        }
    }
}
