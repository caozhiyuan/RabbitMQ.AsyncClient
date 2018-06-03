using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Client.Impl
{
    sealed class BasicCancelOk : Work
    {
        readonly string consumerTag;

        public BasicCancelOk(IBasicConsumer consumer, string consumerTag) : base(consumer)
        {
            this.consumerTag = consumerTag;
        }

        protected override async Task Execute(ModelBase model, IBasicConsumer consumer)
        {
            try
            {
                await consumer.HandleBasicCancelOk(consumerTag).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                var details = new Dictionary<string, object>()
                {
                    {"consumer", consumer},
                    {"context",  "HandleBasicCancelOk"}
                };
                await model.OnCallbackException(CallbackExceptionEventArgs.Build(e, details));
            }
        }
    }
}