using System;
using System.Threading.Tasks;

namespace RabbitMQ.Client.Impl
{
    internal abstract class Work
    {
        readonly IBasicConsumer asyncConsumer;

        protected Work(IBasicConsumer consumer)
        {
            asyncConsumer = (IBasicConsumer)consumer;
        }

        public async Task Execute(ModelBase model)
        {
            try
            {
                await Execute(model, asyncConsumer).ConfigureAwait(false);
            }
            catch (Exception)
            {
               // intentionally caught
            }
        }

        protected abstract Task Execute(ModelBase model, IBasicConsumer consumer);
    }
}
