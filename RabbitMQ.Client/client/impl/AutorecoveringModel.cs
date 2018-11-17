// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 1.1.
//
// The APL v2.0:
//
//---------------------------------------------------------------------------
//   Copyright (c) 2007-2016 Pivotal Software, Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//---------------------------------------------------------------------------
//
// The MPL v1.1:
//
//---------------------------------------------------------------------------
//  The contents of this file are subject to the Mozilla Public License
//  Version 1.1 (the "License"); you may not use this file except in
//  compliance with the License. You may obtain a copy of the License
//  at http://www.mozilla.org/MPL/
//
//  Software distributed under the License is distributed on an "AS IS"
//  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
//  the License for the specific language governing rights and
//  limitations under the License.
//
//  The Original Code is RabbitMQ.
//
//  The Initial Developer of the Original Code is Pivotal Software, Inc.
//  Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
//---------------------------------------------------------------------------

using RabbitMQ.Client.Events;
using RabbitMQ.Client.Framing.Impl;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace RabbitMQ.Client.Impl
{
    public class AutorecoveringModel : IFullModel, IRecoverable
    {
        public readonly object m_eventLock = new object();
        protected AutorecoveringConnection m_connection;
        protected RecoveryAwareModel m_delegate;

        protected List<AsyncEventHandler<BasicAckEventArgs>> m_recordedBasicAckEventHandlers =
            new List<AsyncEventHandler<BasicAckEventArgs>>();

        protected List<AsyncEventHandler<BasicNackEventArgs>> m_recordedBasicNackEventHandlers =
            new List<AsyncEventHandler<BasicNackEventArgs>>();

        protected List<AsyncEventHandler<BasicReturnEventArgs>> m_recordedBasicReturnEventHandlers =
            new List<AsyncEventHandler<BasicReturnEventArgs>>();

        protected List<AsyncEventHandler<CallbackExceptionEventArgs>> m_recordedCallbackExceptionEventHandlers =
            new List<AsyncEventHandler<CallbackExceptionEventArgs>>();

        protected List<AsyncEventHandler<ShutdownEventArgs>> m_recordedShutdownEventHandlers =
            new List<AsyncEventHandler<ShutdownEventArgs>>();

        protected ushort prefetchCountConsumer = 0;
        protected ushort prefetchCountGlobal = 0;
        protected bool usesPublisherConfirms = false;
        protected bool usesTransactions = false;
        private AsyncEventHandler<EventArgs> m_recovery;

        public IConsumerDispatcher ConsumerDispatcher
        {
            get { return m_delegate.ConsumerDispatcher; }
        }

        public TimeSpan ContinuationTimeout
        {
            get { return m_delegate.ContinuationTimeout; }
            set { m_delegate.ContinuationTimeout = value; }
        }

        public AutorecoveringModel(AutorecoveringConnection conn, RecoveryAwareModel _delegate)
        {
            m_connection = conn;
            m_delegate = _delegate;
        }

        public event AsyncEventHandler<BasicAckEventArgs> BasicAcks
        {
            add
            {
                lock (m_eventLock)
                {
                    m_recordedBasicAckEventHandlers.Add(value);
                    m_delegate.BasicAcks += value;
                }
            }
            remove
            {
                lock (m_eventLock)
                {
                    m_recordedBasicAckEventHandlers.Remove(value);
                    m_delegate.BasicAcks -= value;
                }
            }
        }

        public event AsyncEventHandler<BasicNackEventArgs> BasicNacks
        {
            add
            {
                lock (m_eventLock)
                {
                    m_recordedBasicNackEventHandlers.Add(value);
                    m_delegate.BasicNacks += value;
                }
            }
            remove
            {
                lock (m_eventLock)
                {
                    m_recordedBasicNackEventHandlers.Remove(value);
                    m_delegate.BasicNacks -= value;
                }
            }
        }

        public event AsyncEventHandler<EventArgs> BasicRecoverOk
        {
            add
            {
                // TODO: record and re-add handlers
                m_delegate.BasicRecoverOk += value;
            }
            remove { m_delegate.BasicRecoverOk -= value; }
        }

        public event AsyncEventHandler<BasicReturnEventArgs> BasicReturn
        {
            add
            {
                lock (m_eventLock)
                {
                    m_recordedBasicReturnEventHandlers.Add(value);
                    m_delegate.BasicReturn += value;
                }
            }
            remove
            {
                lock (m_eventLock)
                {
                    m_recordedBasicReturnEventHandlers.Remove(value);
                    m_delegate.BasicReturn -= value;
                }
            }
        }

        public event AsyncEventHandler<CallbackExceptionEventArgs> CallbackException
        {
            add
            {
                lock (m_eventLock)
                {
                    m_recordedCallbackExceptionEventHandlers.Add(value);
                    m_delegate.CallbackException += value;
                }
            }
            remove
            {
                lock (m_eventLock)
                {
                    m_recordedCallbackExceptionEventHandlers.Remove(value);
                    m_delegate.CallbackException -= value;
                }
            }
        }

        public event AsyncEventHandler<FlowControlEventArgs> FlowControl
        {
            add
            {
                // TODO: record and re-add handlers
                m_delegate.FlowControl += value;
            }
            remove { m_delegate.FlowControl -= value; }
        }

        public event AsyncEventHandler<ShutdownEventArgs> ModelShutdown
        {
            add
            {
                lock (m_eventLock)
                {
                    m_recordedShutdownEventHandlers.Add(value);
                    m_delegate.ModelShutdown += value;
                }
            }
            remove
            {
                lock (m_eventLock)
                {
                    m_recordedShutdownEventHandlers.Remove(value);
                    m_delegate.ModelShutdown -= value;
                }
            }
        }

        public event AsyncEventHandler<EventArgs> Recovery
        {
            add
            {
                lock (m_eventLock)
                {
                    m_recovery += value;
                }
            }
            remove
            {
                lock (m_eventLock)
                {
                    m_recovery -= value;
                }
            }
        }

        public int ChannelNumber
        {
            get { return m_delegate.ChannelNumber; }
        }

        public ShutdownEventArgs CloseReason
        {
            get { return m_delegate.CloseReason; }
        }

        public IBasicConsumer DefaultConsumer
        {
            get { return m_delegate.DefaultConsumer; }
            set { m_delegate.DefaultConsumer = value; }
        }

        public IModel Delegate
        {
            get { return m_delegate; }
        }

        public bool IsClosed
        {
            get { return m_delegate.IsClosed; }
        }

        public bool IsOpen
        {
            get { return m_delegate.IsOpen; }
        }

        public ulong NextPublishSeqNo
        {
            get { return m_delegate.NextPublishSeqNo; }
        }

        public async Task AutomaticallyRecover(AutorecoveringConnection conn, IConnection connDelegate)
        {
            m_connection = conn;
            RecoveryAwareModel defunctModel = m_delegate;

            m_delegate = await conn.CreateNonRecoveringModel();
            m_delegate.InheritOffsetFrom(defunctModel);

            RecoverModelShutdownHandlers();
            await RecoverState();

            RecoverBasicReturnHandlers();
            RecoverBasicAckHandlers();
            RecoverBasicNackHandlers();
            RecoverCallbackExceptionHandlers();

            await RunRecoveryEventHandlers();
        }

        public Task BasicQos(ushort prefetchCount,
            bool global)
        {
            return m_delegate.BasicQos(0, prefetchCount, global);
        }

        public async Task Close(ushort replyCode, string replyText, bool abort)
        {
            try
            {
                await m_delegate.Close(replyCode, replyText, abort);
            }
            finally
            {
                m_connection.UnregisterModel(this);
            }
        }

        public async Task Close(ShutdownEventArgs reason, bool abort)
        {
            try
            {
                await m_delegate.Close(reason, abort);
            }
            finally
            {
                m_connection.UnregisterModel(this);
            }
        }

        public Task<bool> DispatchAsynchronous(Command cmd)
        {
            return m_delegate.DispatchAsynchronous(cmd);
        }

        public Task FinishClose()
        {
            return m_delegate.FinishClose();
        }

        public Task HandleCommand(ISession session, Command cmd)
        {
            return m_delegate.HandleCommand(session, cmd);
        }

        public virtual Task OnBasicAck(BasicAckEventArgs args)
        {
            return m_delegate.OnBasicAck(args);
        }

        public virtual Task OnBasicNack(BasicNackEventArgs args)
        {
            return m_delegate.OnBasicNack(args);
        }

        public virtual Task OnBasicRecoverOk(EventArgs args)
        {
            return m_delegate.OnBasicRecoverOk(args);
        }

        public virtual Task OnBasicReturn(BasicReturnEventArgs args)
        {
            return m_delegate.OnBasicReturn(args);
        }

        public virtual Task OnCallbackException(CallbackExceptionEventArgs args)
        {
            return m_delegate.OnCallbackException(args);
        }

        public virtual Task OnFlowControl(FlowControlEventArgs args)
        {
            return m_delegate.OnFlowControl(args);
        }

        public virtual Task OnModelShutdown(ShutdownEventArgs reason)
        {
            return m_delegate.OnModelShutdown(reason);
        }

        public Task OnSessionShutdown(ISession session, ShutdownEventArgs reason)
        {
            return m_delegate.OnSessionShutdown(session, reason);
        }

        public bool SetCloseReason(ShutdownEventArgs reason)
        {
            return m_delegate.SetCloseReason(reason);
        }

        public override string ToString()
        {
            return m_delegate.ToString();
        }

        public Task Dispose()
        {
            return Abort();
        }

        public Task ConnectionTuneOk(ushort channelMax,
            uint frameMax,
            ushort heartbeat)
        {
            return m_delegate.ConnectionTuneOk(channelMax, frameMax, heartbeat);
        }

        public Task HandleBasicAck(ulong deliveryTag,
            bool multiple)
        {
            return m_delegate.HandleBasicAck(deliveryTag, multiple);
        }

        public void HandleBasicCancel(string consumerTag, bool nowait)
        {
            m_delegate.HandleBasicCancel(consumerTag, nowait);
        }

        public void HandleBasicCancelOk(string consumerTag)
        {
            m_delegate.HandleBasicCancelOk(consumerTag);
        }

        public void HandleBasicConsumeOk(string consumerTag)
        {
            m_delegate.HandleBasicConsumeOk(consumerTag);
        }

        public void HandleBasicDeliver(string consumerTag,
            ulong deliveryTag,
            bool redelivered,
            string exchange,
            string routingKey,
            IBasicProperties basicProperties,
            byte[] body)
        {
            m_delegate.HandleBasicDeliver(consumerTag, deliveryTag, redelivered, exchange,
                routingKey, basicProperties, body);
        }

        public void HandleBasicGetEmpty()
        {
            m_delegate.HandleBasicGetEmpty();
        }

        public void HandleBasicGetOk(ulong deliveryTag,
            bool redelivered,
            string exchange,
            string routingKey,
            uint messageCount,
            IBasicProperties basicProperties,
            byte[] body)
        {
            m_delegate.HandleBasicGetOk(deliveryTag, redelivered, exchange, routingKey,
                messageCount, basicProperties, body);
        }

        public Task HandleBasicNack(ulong deliveryTag,
            bool multiple,
            bool requeue)
        {
           return  m_delegate.HandleBasicNack(deliveryTag, multiple, requeue);
        }

        public Task HandleBasicRecoverOk()
        {
            return m_delegate.HandleBasicRecoverOk();
        }

        public Task HandleBasicReturn(ushort replyCode,
            string replyText,
            string exchange,
            string routingKey,
            IBasicProperties basicProperties,
            byte[] body)
        {
            return m_delegate.HandleBasicReturn(replyCode, replyText, exchange,
                routingKey, basicProperties, body);
        }

        public Task HandleChannelClose(ushort replyCode,
            string replyText,
            ushort classId,
            ushort methodId)
        {
            return m_delegate.HandleChannelClose(replyCode, replyText, classId, methodId);
        }

        public Task HandleChannelCloseOk()
        {
            return m_delegate.HandleChannelCloseOk();
        }

        public Task HandleChannelFlow(bool active)
        {
            return m_delegate.HandleChannelFlow(active);
        }

        public Task HandleConnectionBlocked(string reason)
        {
            return m_delegate.HandleConnectionBlocked(reason);
        }

        public Task HandleConnectionClose(ushort replyCode,
            string replyText,
            ushort classId,
            ushort methodId)
        {
            return m_delegate.HandleConnectionClose(replyCode, replyText, classId, methodId);
        }

        public void HandleConnectionOpenOk(string knownHosts)
        {
            m_delegate.HandleConnectionOpenOk(knownHosts);
        }

        public void HandleConnectionSecure(byte[] challenge)
        {
            m_delegate.HandleConnectionSecure(challenge);
        }

        public void HandleConnectionStart(byte versionMajor,
            byte versionMinor,
            IDictionary<string, object> serverProperties,
            byte[] mechanisms,
            byte[] locales)
        {
            m_delegate.HandleConnectionStart(versionMajor, versionMinor, serverProperties,
                mechanisms, locales);
        }

        public void HandleConnectionTune(ushort channelMax,
            uint frameMax,
            ushort heartbeat)
        {
            m_delegate.HandleConnectionTune(channelMax, frameMax, heartbeat);
        }

        public Task HandleConnectionUnblocked()
        {
            return m_delegate.HandleConnectionUnblocked();
        }

        public void HandleQueueDeclareOk(string queue,
            uint messageCount,
            uint consumerCount)
        {
            m_delegate.HandleQueueDeclareOk(queue, messageCount, consumerCount);
        }

        public Task _Private_BasicCancel(string consumerTag,
            bool nowait)
        {
            return m_delegate._Private_BasicCancel(consumerTag,
                nowait);
        }

        public Task _Private_BasicConsume(string queue,
            string consumerTag,
            bool noLocal,
            bool autoAck,
            bool exclusive,
            bool nowait,
            IDictionary<string, object> arguments)
        {
            return m_delegate._Private_BasicConsume(queue,
                consumerTag,
                noLocal,
                autoAck,
                exclusive,
                nowait,
                arguments);
        }

        public Task _Private_BasicGet(string queue, bool autoAck)
        {
            return m_delegate._Private_BasicGet(queue, autoAck);
        }

        public Task _Private_BasicPublish(string exchange,
            string routingKey,
            bool mandatory,
            IBasicProperties basicProperties,
            byte[] body)
        {
            return m_delegate._Private_BasicPublish(exchange, routingKey, mandatory,
                basicProperties, body);
        }

        public Task _Private_BasicRecover(bool requeue)
        {
            return m_delegate._Private_BasicRecover(requeue);
        }

        public Task _Private_ChannelClose(ushort replyCode,
            string replyText,
            ushort classId,
            ushort methodId)
        {
            return m_delegate._Private_ChannelClose(replyCode, replyText,
                classId, methodId);
        }

        public Task _Private_ChannelCloseOk()
        {
            return m_delegate._Private_ChannelCloseOk();
        }

        public Task _Private_ChannelFlowOk(bool active)
        {
            return m_delegate._Private_ChannelFlowOk(active);
        }

        public Task _Private_ChannelOpen(string outOfBand)
        {
            return m_delegate._Private_ChannelOpen(outOfBand);
        }

        public Task _Private_ConfirmSelect(bool nowait)
        {
            return m_delegate._Private_ConfirmSelect(nowait);
        }

        public Task _Private_ConnectionClose(ushort replyCode,
            string replyText,
            ushort classId,
            ushort methodId)
        {
            return m_delegate._Private_ConnectionClose(replyCode, replyText,
                classId, methodId);
        }

        public Task _Private_ConnectionCloseOk()
        {
            return m_delegate._Private_ConnectionCloseOk();
        }

        public Task _Private_ConnectionOpen(string virtualHost,
            string capabilities,
            bool insist)
        {
            return m_delegate._Private_ConnectionOpen(virtualHost, capabilities, insist);
        }

        public Task _Private_ConnectionSecureOk(byte[] response)
        {
            return m_delegate._Private_ConnectionSecureOk(response);
        }

        public Task _Private_ConnectionStartOk(IDictionary<string, object> clientProperties,
            string mechanism, byte[] response, string locale)
        {
            return m_delegate._Private_ConnectionStartOk(clientProperties, mechanism,
                response, locale);
        }

        public Task _Private_ExchangeBind(string destination,
            string source,
            string routingKey,
            bool nowait,
            IDictionary<string, object> arguments)
        {
            return _Private_ExchangeBind(destination, source, routingKey,
                nowait, arguments);
        }

        public Task _Private_ExchangeDeclare(string exchange,
            string type,
            bool passive,
            bool durable,
            bool autoDelete,
            bool @internal,
            bool nowait,
            IDictionary<string, object> arguments)
        {
            return _Private_ExchangeDeclare(exchange, type, passive,
                durable, autoDelete, @internal,
                nowait, arguments);
        }

        public Task _Private_ExchangeDelete(string exchange,
            bool ifUnused,
            bool nowait)
        {
            return _Private_ExchangeDelete(exchange, ifUnused, nowait);
        }

        public Task _Private_ExchangeUnbind(string destination,
            string source,
            string routingKey,
            bool nowait,
            IDictionary<string, object> arguments)
        {
            return m_delegate._Private_ExchangeUnbind(destination, source, routingKey,
                nowait, arguments);
        }

        public Task _Private_QueueBind(string queue,
            string exchange,
            string routingKey,
            bool nowait,
            IDictionary<string, object> arguments)
        {
            return _Private_QueueBind(queue, exchange, routingKey,
                nowait, arguments);
        }

        public Task _Private_QueueDeclare(string queue,
            bool passive,
            bool durable,
            bool exclusive,
            bool autoDelete,
            bool nowait,
            IDictionary<string, object> arguments)
        {
            return m_delegate._Private_QueueDeclare(queue, passive,
                durable, exclusive, autoDelete,
                nowait, arguments);
        }

        public Task<uint> _Private_QueueDelete(string queue,
            bool ifUnused,
            bool ifEmpty,
            bool nowait)
        {
            return m_delegate._Private_QueueDelete(queue, ifUnused,
                ifEmpty, nowait);
        }

        public Task<uint> _Private_QueuePurge(string queue,
            bool nowait)
        {
            return m_delegate._Private_QueuePurge(queue, nowait);
        }

        public async Task Abort()
        {
            try
            {
                await m_delegate.Abort();
            }
            finally
            {
                m_connection.UnregisterModel(this);
            }
        }

        public async Task Abort(ushort replyCode, string replyText)
        {
            try
            {
                await m_delegate.Abort(replyCode, replyText);
            }
            finally
            {
                m_connection.UnregisterModel(this);
            }
        }

        public Task BasicAck(ulong deliveryTag,
            bool multiple)
        {
            return m_delegate.BasicAck(deliveryTag, multiple);
        }

        public Task BasicCancel(string consumerTag)
        {
            RecordedConsumer cons = m_connection.DeleteRecordedConsumer(consumerTag);
            if (cons != null)
            {
                m_connection.MaybeDeleteRecordedAutoDeleteQueue(cons.Queue);
            }
            return m_delegate.BasicCancel(consumerTag);
        }

        public async Task<string> BasicConsume(
            string queue,
            bool autoAck,
            string consumerTag,
            bool noLocal,
            bool exclusive,
            IDictionary<string, object> arguments,
            IBasicConsumer consumer)
        {
            var result = await m_delegate.BasicConsume(queue, autoAck, consumerTag, noLocal,
                exclusive, arguments, consumer);
            RecordedConsumer rc = new RecordedConsumer(this, queue).
                WithConsumerTag(result).
                WithConsumer(consumer).
                WithExclusive(exclusive).
                WithAutoAck(autoAck).
                WithArguments(arguments);
            m_connection.RecordConsumer(result, rc);
            return result;
        }

        public Task<BasicGetResult> BasicGet(string queue,
            bool autoAck)
        {
            return m_delegate.BasicGet(queue, autoAck);
        }

        public Task BasicNack(ulong deliveryTag,
            bool multiple,
            bool requeue)
        {
            return m_delegate.BasicNack(deliveryTag, multiple, requeue);
        }

        public Task BasicPublish(string exchange,
            string routingKey,
            bool mandatory,
            IBasicProperties basicProperties,
            byte[] body)
        {
            return m_delegate.BasicPublish(exchange,
                routingKey,
                mandatory,
                basicProperties,
                body);
        }

        public Task BasicQos(uint prefetchSize,
            ushort prefetchCount,
            bool global)
        {
            if (global)
            {
                prefetchCountGlobal = prefetchCount;
            }
            else
            {
                prefetchCountConsumer = prefetchCount;
            }
            return m_delegate.BasicQos(prefetchSize, prefetchCount, global);
        }

        public Task BasicRecover(bool requeue)
        {
            return m_delegate.BasicRecover(requeue);
        }

        public Task BasicRecoverAsync(bool requeue)
        {
            return m_delegate.BasicRecoverAsync(requeue);
        }

        public Task BasicReject(ulong deliveryTag,
            bool requeue)
        {
            return m_delegate.BasicReject(deliveryTag, requeue);
        }

        public void Close()
        {
            try
            {
                m_delegate.Close();
            }
            finally
            {
                m_connection.UnregisterModel(this);
            }
        }

        public void Close(ushort replyCode, string replyText)
        {
            try
            {
                m_delegate.Close(replyCode, replyText);
            }
            finally
            {
                m_connection.UnregisterModel(this);
            }
        }

        public Task ConfirmSelect()
        {
            usesPublisherConfirms = true;
            return m_delegate.ConfirmSelect();
        }

        public IBasicProperties CreateBasicProperties()
        {
            return m_delegate.CreateBasicProperties();
        }

        public Task ExchangeBind(string destination,
            string source,
            string routingKey,
            IDictionary<string, object> arguments)
        {
            RecordedBinding eb = new RecordedExchangeBinding(this).
                WithSource(source).
                WithDestination(destination).
                WithRoutingKey(routingKey).
                WithArguments(arguments);
            m_connection.RecordBinding(eb);
            return m_delegate.ExchangeBind(destination, source, routingKey, arguments);
        }

        public Task ExchangeBindNoWait(string destination,
            string source,
            string routingKey,
            IDictionary<string, object> arguments)
        {
            return m_delegate.ExchangeBindNoWait(destination, source, routingKey, arguments);
        }

        public async Task ExchangeDeclare(string exchange, string type, bool durable,
            bool autoDelete, IDictionary<string, object> arguments)
        {
            RecordedExchange rx = new RecordedExchange(this, exchange).
                WithType(type).
                WithDurable(durable).
                WithAutoDelete(autoDelete).
                WithArguments(arguments);
            await m_delegate.ExchangeDeclare(exchange, type, durable,
                autoDelete, arguments);
            m_connection.RecordExchange(exchange, rx);
        }

        public async Task ExchangeDeclareNoWait(string exchange,
            string type,
            bool durable,
            bool autoDelete,
            IDictionary<string, object> arguments)
        {
            RecordedExchange rx = new RecordedExchange(this, exchange).
                WithType(type).
                WithDurable(durable).
                WithAutoDelete(autoDelete).
                WithArguments(arguments);
            await m_delegate.ExchangeDeclareNoWait(exchange, type, durable,
                autoDelete, arguments);
            m_connection.RecordExchange(exchange, rx);
        }

        public Task ExchangeDeclarePassive(string exchange)
        {
            return m_delegate.ExchangeDeclarePassive(exchange);
        }

        public async Task ExchangeDelete(string exchange,
            bool ifUnused)
        {
            await m_delegate.ExchangeDelete(exchange, ifUnused);
            m_connection.DeleteRecordedExchange(exchange);
        }

        public async Task ExchangeDeleteNoWait(string exchange,
            bool ifUnused)
        {
            await m_delegate.ExchangeDeleteNoWait(exchange, ifUnused);
            m_connection.DeleteRecordedExchange(exchange);
        }

        public async Task ExchangeUnbind(string destination,
            string source,
            string routingKey,
            IDictionary<string, object> arguments)
        {
            RecordedBinding eb = new RecordedExchangeBinding(this).
                WithSource(source).
                WithDestination(destination).
                WithRoutingKey(routingKey).
                WithArguments(arguments);
            m_connection.DeleteRecordedBinding(eb);
            await m_delegate.ExchangeUnbind(destination, source, routingKey, arguments);
            m_connection.MaybeDeleteRecordedAutoDeleteExchange(source);
        }

        public Task ExchangeUnbindNoWait(string destination,
            string source,
            string routingKey,
            IDictionary<string, object> arguments)
        {
            return m_delegate.ExchangeUnbind(destination, source, routingKey, arguments);
        }

        public Task QueueBind(string queue,
            string exchange,
            string routingKey,
            IDictionary<string, object> arguments)
        {
            RecordedBinding qb = new RecordedQueueBinding(this).
                WithSource(exchange).
                WithDestination(queue).
                WithRoutingKey(routingKey).
                WithArguments(arguments);
            m_connection.RecordBinding(qb);
            return m_delegate.QueueBind(queue, exchange, routingKey, arguments);
        }

        public Task QueueBindNoWait(string queue,
            string exchange,
            string routingKey,
            IDictionary<string, object> arguments)
        {
            return m_delegate.QueueBind(queue, exchange, routingKey, arguments);
        }

        public async Task<QueueDeclareOk> QueueDeclare(string queue, bool durable,
                                           bool exclusive, bool autoDelete,
                                           IDictionary<string, object> arguments)
        {
            var result = await m_delegate.QueueDeclare(queue, durable, exclusive,
                autoDelete, arguments);
            RecordedQueue rq = new RecordedQueue(this, result.QueueName).
                Durable(durable).
                Exclusive(exclusive).
                AutoDelete(autoDelete).
                Arguments(arguments).
                ServerNamed(string.Empty.Equals(queue));
            m_connection.RecordQueue(result.QueueName, rq);
            return result;
        }

        public async Task QueueDeclareNoWait(string queue, bool durable,
                                       bool exclusive, bool autoDelete,
                                       IDictionary<string, object> arguments)
        {
            await m_delegate.QueueDeclareNoWait(queue, durable, exclusive,
                autoDelete, arguments);
            RecordedQueue rq = new RecordedQueue(this, queue).
                Durable(durable).
                Exclusive(exclusive).
                AutoDelete(autoDelete).
                Arguments(arguments).
                ServerNamed(string.Empty.Equals(queue));
            m_connection.RecordQueue(queue, rq);
        }

        public Task<QueueDeclareOk> QueueDeclarePassive(string queue)
        {
            return m_delegate.QueueDeclarePassive(queue);
        }

        public Task<uint> MessageCount(string queue)
        {
            return m_delegate.MessageCount(queue);
        }

        public Task<uint> ConsumerCount(string queue)
        {
            return m_delegate.ConsumerCount(queue);
        }

        public async Task<uint> QueueDelete(string queue,
            bool ifUnused,
            bool ifEmpty)
        {
            var result = await m_delegate.QueueDelete(queue, ifUnused, ifEmpty);
            m_connection.DeleteRecordedQueue(queue);
            return result;
        }

        public async Task QueueDeleteNoWait(string queue,
            bool ifUnused,
            bool ifEmpty)
        {
            await m_delegate.QueueDeleteNoWait(queue, ifUnused, ifEmpty);
            m_connection.DeleteRecordedQueue(queue);
        }

        public Task<uint> QueuePurge(string queue)
        {
            return m_delegate.QueuePurge(queue);
        }

        public async Task QueueUnbind(string queue,
            string exchange,
            string routingKey,
            IDictionary<string, object> arguments)
        {
            RecordedBinding qb = new RecordedQueueBinding(this).
                WithSource(exchange).
                WithDestination(queue).
                WithRoutingKey(routingKey).
                WithArguments(arguments);
            m_connection.DeleteRecordedBinding(qb);
            await m_delegate.QueueUnbind(queue, exchange, routingKey, arguments);
            m_connection.MaybeDeleteRecordedAutoDeleteExchange(exchange);
        }

        public Task TxCommit()
        {
            return m_delegate.TxCommit();
        }

        public Task TxRollback()
        {
            return m_delegate.TxRollback();
        }

        public Task TxSelect()
        {
            usesTransactions = true;
            return m_delegate.TxSelect();
        }

        public Task<Tuple<bool, bool>> WaitForConfirms0(TimeSpan timeout)
        {
            return m_delegate.WaitForConfirms0(timeout);
        }

        public Task<bool> WaitForConfirms(TimeSpan timeout)
        {
            return m_delegate.WaitForConfirms(timeout);
        }

        public Task<bool> WaitForConfirms()
        {
            return m_delegate.WaitForConfirms();
        }

        public Task WaitForConfirmsOrDie()
        {
            return m_delegate.WaitForConfirmsOrDie();
        }

        public Task WaitForConfirmsOrDie(TimeSpan timeout)
        {
            return m_delegate.WaitForConfirmsOrDie(timeout);
        }

        protected void RecoverBasicAckHandlers()
        {
            List<AsyncEventHandler<BasicAckEventArgs>> handler = m_recordedBasicAckEventHandlers;
            if (handler != null)
            {
                foreach (AsyncEventHandler<BasicAckEventArgs> eh in handler)
                {
                    m_delegate.BasicAcks += eh;
                }
            }
        }

        protected void RecoverBasicNackHandlers()
        {
            List<AsyncEventHandler<BasicNackEventArgs>> handler = m_recordedBasicNackEventHandlers;
            if (handler != null)
            {
                foreach (AsyncEventHandler<BasicNackEventArgs> eh in handler)
                {
                    m_delegate.BasicNacks += eh;
                }
            }
        }

        protected void RecoverBasicReturnHandlers()
        {
            List<AsyncEventHandler<BasicReturnEventArgs>> handler = m_recordedBasicReturnEventHandlers;
            if (handler != null)
            {
                foreach (AsyncEventHandler<BasicReturnEventArgs> eh in handler)
                {
                    m_delegate.BasicReturn += eh;
                }
            }
        }

        protected void RecoverCallbackExceptionHandlers()
        {
            List<AsyncEventHandler<CallbackExceptionEventArgs>> handler = m_recordedCallbackExceptionEventHandlers;
            if (handler != null)
            {
                foreach (AsyncEventHandler<CallbackExceptionEventArgs> eh in handler)
                {
                    m_delegate.CallbackException += eh;
                }
            }
        }

        protected void RecoverModelShutdownHandlers()
        {
            List<AsyncEventHandler<ShutdownEventArgs>> handler = m_recordedShutdownEventHandlers;
            if (handler != null)
            {
                foreach (AsyncEventHandler<ShutdownEventArgs> eh in handler)
                {
                    m_delegate.ModelShutdown += eh;
                }
            }
        }

        protected async Task RecoverState()
        {
            if (prefetchCountConsumer != 0)
            {
                await BasicQos(prefetchCountConsumer, false);
            }

            if (prefetchCountGlobal != 0)
            {
                await BasicQos(prefetchCountGlobal, true);
            }

            if (usesPublisherConfirms)
            {
                ConfirmSelect();
            }

            if (usesTransactions)
            {
                await TxSelect();
            }
        }

        protected async Task RunRecoveryEventHandlers()
        {
            AsyncEventHandler<EventArgs> handler = m_recovery;
            if (handler != null)
            {
                foreach (AsyncEventHandler<EventArgs> reh in handler.GetInvocationList())
                {
                    try
                    {
                        await reh(this, EventArgs.Empty);
                    }
                    catch (Exception e)
                    {
                        var args = new CallbackExceptionEventArgs(e);
                        args.Detail["context"] = "OnModelRecovery";
                        await m_delegate.OnCallbackException(args);
                    }
                }
            }
        }

        public IBasicPublishBatch CreateBasicPublishBatch()
        {
            return ((IFullModel)m_delegate).CreateBasicPublishBatch();
        }
    }
}
