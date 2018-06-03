using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Client.Impl
{
    sealed class BasicCancel : Work
    {
        readonly string consumerTag;

        public BasicCancel(IBasicConsumer consumer, string consumerTag) : base(consumer)
        {
            this.consumerTag = consumerTag;
        }

        protected override async Task Execute(ModelBase model, IBasicConsumer consumer)
        {
            try
            {
                await consumer.HandleBasicCancel(consumerTag).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                var details = new Dictionary<string, object>
                {
                    {"consumer", consumer},
                    {"context",  "HandleBasicCancel"}
                };
                await model.OnCallbackException(CallbackExceptionEventArgs.Build(e, details));
            }
        }
    }
}