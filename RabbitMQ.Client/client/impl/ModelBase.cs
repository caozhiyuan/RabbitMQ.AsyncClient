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
using RabbitMQ.Client.Exceptions;
using RabbitMQ.Client.Framing;
using RabbitMQ.Client.Framing.Impl;
using RabbitMQ.Util;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

#if (NETFX_CORE)
using Trace = System.Diagnostics.Debug;
#endif

namespace RabbitMQ.Client.Impl
{
    public abstract class ModelBase : IFullModel, IRecoverable
    {
        public readonly IDictionary<string, IBasicConsumer> m_consumers = new Dictionary<string, IBasicConsumer>();

        ///<summary>Only used to kick-start a connection open
        ///sequence. See <see cref="Connection.Open"/> </summary>
        public TaskCompletionSource<ConnectionStartDetails> m_connectionStartCell = null;

        private TimeSpan m_handshakeContinuationTimeout = TimeSpan.FromSeconds(10);
        private TimeSpan m_continuationTimeout = TimeSpan.FromSeconds(20);

        private RpcContinuationQueue m_continuationQueue = new RpcContinuationQueue();
        private ManualResetEvent m_flowControlBlock = new ManualResetEvent(true);

        private readonly object m_eventLock = new object();
        private readonly object m_flowSendLock = new object();
        private readonly object m_shutdownLock = new object();

        private readonly SynchronizedList<ulong> m_unconfirmedSet = new SynchronizedList<ulong>();

        private AsyncEventHandler<BasicAckEventArgs> m_basicAck;
        private AsyncEventHandler<BasicNackEventArgs> m_basicNack;
        private AsyncEventHandler<EventArgs> m_basicRecoverOk;
        private AsyncEventHandler<BasicReturnEventArgs> m_basicReturn;
        private AsyncEventHandler<CallbackExceptionEventArgs> m_callbackException;
        private AsyncEventHandler<FlowControlEventArgs> m_flowControl;
        private AsyncEventHandler<ShutdownEventArgs> m_modelShutdown;

        private bool m_onlyAcksReceived = true;

        private AsyncEventHandler<EventArgs> m_recovery;

        public IConsumerDispatcher ConsumerDispatcher { get; private set; }

        public ModelBase(ISession session)
            : this(session, session.Connection.ConsumerWorkService)
        { }

        public ModelBase(ISession session, ConsumerWorkService workService)
        {
            ConsumerDispatcher = new AsyncConsumerDispatcher(this, workService);

            Initialise(session);
        }

        protected void Initialise(ISession session)
        {
            CloseReason = null;
            NextPublishSeqNo = 0;
            Session = session;
            Session.CommandReceived = HandleCommand;
            Session.SessionShutdown += OnSessionShutdown;
        }

        public TimeSpan HandshakeContinuationTimeout
        {
            get { return m_handshakeContinuationTimeout; }
            set { m_handshakeContinuationTimeout = value; }
        }

        public TimeSpan ContinuationTimeout
        {
            get { return m_continuationTimeout; }
            set { m_continuationTimeout = value; }
        }

        public event AsyncEventHandler<BasicAckEventArgs> BasicAcks
        {
            add
            {
                lock (m_eventLock)
                {
                    m_basicAck += value;
                }
            }
            remove
            {
                lock (m_eventLock)
                {
                    m_basicAck -= value;
                }
            }
        }

        public event AsyncEventHandler<BasicNackEventArgs> BasicNacks
        {
            add
            {
                lock (m_eventLock)
                {
                    m_basicNack += value;
                }
            }
            remove
            {
                lock (m_eventLock)
                {
                    m_basicNack -= value;
                }
            }
        }

        public event AsyncEventHandler<EventArgs> BasicRecoverOk
        {
            add
            {
                lock (m_eventLock)
                {
                    m_basicRecoverOk += value;
                }
            }
            remove
            {
                lock (m_eventLock)
                {
                    m_basicRecoverOk -= value;
                }
            }
        }

        public event AsyncEventHandler<BasicReturnEventArgs> BasicReturn
        {
            add
            {
                lock (m_eventLock)
                {
                    m_basicReturn += value;
                }
            }
            remove
            {
                lock (m_eventLock)
                {
                    m_basicReturn -= value;
                }
            }
        }

        public event AsyncEventHandler<CallbackExceptionEventArgs> CallbackException
        {
            add
            {
                lock (m_eventLock)
                {
                    m_callbackException += value;
                }
            }
            remove
            {
                lock (m_eventLock)
                {
                    m_callbackException -= value;
                }
            }
        }

        public event AsyncEventHandler<FlowControlEventArgs> FlowControl
        {
            add
            {
                lock (m_eventLock)
                {
                    m_flowControl += value;
                }
            }
            remove
            {
                lock (m_eventLock)
                {
                    m_flowControl -= value;
                }
            }
        }

        public event AsyncEventHandler<ShutdownEventArgs> ModelShutdown
        {
            add
            {
                bool ok = false;
                if (CloseReason == null)
                {
                    lock (m_shutdownLock)
                    {
                        if (CloseReason == null)
                        {
                            m_modelShutdown += value;
                            ok = true;
                        }
                    }
                }
                if (!ok)
                {
                    value(this, CloseReason);
                }
            }
            remove
            {
                lock (m_shutdownLock)
                {
                    m_modelShutdown -= value;
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
            get { return ((Session)Session).ChannelNumber; }
        }

        public ShutdownEventArgs CloseReason { get; private set; }

        public IBasicConsumer DefaultConsumer { get; set; }

        public bool IsClosed
        {
            get { return !IsOpen; }
        }

        public bool IsOpen
        {
            get { return CloseReason == null; }
        }

        public ulong NextPublishSeqNo { get; private set; }

        public ISession Session { get; private set; }

        public Task Close(ushort replyCode, string replyText, bool abort)
        {
            return Close(new ShutdownEventArgs(ShutdownInitiator.Application,
                replyCode, replyText),
                abort);
        }

        public async Task Close(ShutdownEventArgs reason, bool abort)
        {
            var k = new ShutdownContinuation();
            ModelShutdown += k.OnConnectionShutdown;

            try
            {
                ConsumerDispatcher.Quiesce();
                if (SetCloseReason(reason))
                {
                    await _Private_ChannelClose(reason.ReplyCode, reason.ReplyText, 0, 0);
                }                
                await k.Wait(TimeSpan.FromMilliseconds(10000));
                await ConsumerDispatcher.Shutdown(this);
            }
            catch (AlreadyClosedException)
            {
                if (!abort)
                {
                    throw;
                }
            }
            catch (IOException)
            {
                if (!abort)
                {
                    throw;
                }
            }
            catch (Exception)
            {
                if (!abort)
                {
                    throw;
                }
            }
        }

        public async Task<string> ConnectionOpen(string virtualHost,
            string capabilities,
            bool insist)
        {
            var k = new ConnectionOpenContinuation();
            Enqueue(k);
            try
            {
                await _Private_ConnectionOpen(virtualHost, capabilities, insist);
            }
            catch (AlreadyClosedException)
            {
                // let continuation throw OperationInterruptedException,
                // which is a much more suitable exception before connection
                // negotiation finishes
            }
            await k.GetReply(HandshakeContinuationTimeout);
            return k.m_knownHosts;
        }

        public async Task<ConnectionSecureOrTune> ConnectionSecureOk(byte[] response)
        {
            var k = new ConnectionStartRpcContinuation();
            Enqueue(k);
            try
            {
                await _Private_ConnectionSecureOk(response);
            }
            catch (AlreadyClosedException)
            {
                // let continuation throw OperationInterruptedException,
                // which is a much more suitable exception before connection
                // negotiation finishes
            }
            await k.GetReply(HandshakeContinuationTimeout);
            return k.m_result;
        }

        public async Task<ConnectionSecureOrTune> ConnectionStartOk(IDictionary<string, object> clientProperties,
            string mechanism,
            byte[] response,
            string locale)
        {
            var k = new ConnectionStartRpcContinuation();
            Enqueue(k);
            try
            {
                await _Private_ConnectionStartOk(clientProperties, mechanism,
                    response, locale);
            }
            catch (AlreadyClosedException)
            {
                // let continuation throw OperationInterruptedException,
                // which is a much more suitable exception before connection
                // negotiation finishes
            }
            await k.GetReply(HandshakeContinuationTimeout);
            return k.m_result;
        }

        public abstract Task<bool> DispatchAsynchronous(Command cmd);

        public void Enqueue(IRpcContinuation k)
        {
            bool ok = false;
            if (CloseReason == null)
            {
                lock (m_shutdownLock)
                {
                    if (CloseReason == null)
                    {
                        m_continuationQueue.Enqueue(k);
                        ok = true;
                    }
                }
            }
            if (!ok)
            {
                k.HandleModelShutdown(CloseReason);
            }
        }

        public async Task FinishClose()
        {
            if (CloseReason != null)
            {
                await Session.Close(CloseReason);
            }
            if (m_connectionStartCell != null)
            {
                m_connectionStartCell.TrySetResult(null);
            }
        }

        public async Task HandleCommand(ISession session, Command cmd)
        {
            if (!await DispatchAsynchronous(cmd))// Was asynchronous. Already processed. No need to process further.
                m_continuationQueue.Next().HandleCommand(cmd);
        }

        public async Task<MethodBase> ModelRpc(MethodBase method, ContentHeaderBase header, byte[] body)
        {
            var k = new SimpleBlockingRpcContinuation();
            await TransmitAndEnqueue(new Command(method, header, body), k);
            return (await k.GetReply(this.ContinuationTimeout)).Method;
        }

        public Task ModelSend(MethodBase method, ContentHeaderBase header, byte[] body)
        {
            if (method.HasContent)
            {
                m_flowControlBlock.WaitOne();
                return Session.Transmit(new Command(method, header, body));
            }
            else
            {
                return Session.Transmit(new Command(method, header, body));
            }
        }

        public virtual async Task OnBasicAck(BasicAckEventArgs args)
        {
            AsyncEventHandler<BasicAckEventArgs> handler;
            lock (m_eventLock)
            {
                handler = m_basicAck;
            }
            if (handler != null)
            {
                foreach (AsyncEventHandler<BasicAckEventArgs> h in handler.GetInvocationList())
                {
                    try
                    {
                        await h(this, args);
                    }
                    catch (Exception e)
                    {
                        await OnCallbackException(CallbackExceptionEventArgs.Build(e, "OnBasicAck"));
                    }
                }
            }

            handleAckNack(args.DeliveryTag, args.Multiple, false);
        }

        public virtual async Task OnBasicNack(BasicNackEventArgs args)
        {
            AsyncEventHandler<BasicNackEventArgs> handler;
            lock (m_eventLock)
            {
                handler = m_basicNack;
            }
            if (handler != null)
            {
                foreach (AsyncEventHandler<BasicNackEventArgs> h in handler.GetInvocationList())
                {
                    try
                    {
                        await h(this, args);
                    }
                    catch (Exception e)
                    {
                        await OnCallbackException(CallbackExceptionEventArgs.Build(e, "OnBasicNack"));
                    }
                }
            }

            handleAckNack(args.DeliveryTag, args.Multiple, true);
        }

        public virtual async Task OnBasicRecoverOk(EventArgs args)
        {
            AsyncEventHandler<EventArgs> handler;
            lock (m_eventLock)
            {
                handler = m_basicRecoverOk;
            }
            if (handler != null)
            {
                foreach (AsyncEventHandler<EventArgs> h in handler.GetInvocationList())
                {
                    try
                    {
                        await h(this, args);
                    }
                    catch (Exception e)
                    {
                        await OnCallbackException(CallbackExceptionEventArgs.Build(e, "OnBasicRecover"));
                    }
                }
            }
        }

        public virtual async Task OnBasicReturn(BasicReturnEventArgs args)
        {
            AsyncEventHandler<BasicReturnEventArgs> handler;
            lock (m_eventLock)
            {
                handler = m_basicReturn;
            }
            if (handler != null)
            {
                foreach (AsyncEventHandler<BasicReturnEventArgs> h in handler.GetInvocationList())
                {
                    try
                    {
                        await h(this, args);
                    }
                    catch (Exception e)
                    {
                        await OnCallbackException(CallbackExceptionEventArgs.Build(e, "OnBasicReturn"));
                    }
                }
            }
        }

        public virtual async Task OnCallbackException(CallbackExceptionEventArgs args)
        {
            AsyncEventHandler<CallbackExceptionEventArgs> handler;
            lock (m_eventLock)
            {
                handler = m_callbackException;
            }
            if (handler != null)
            {
                foreach (AsyncEventHandler<CallbackExceptionEventArgs> h in handler.GetInvocationList())
                {
                    try
                    {
                        await h(this, args);
                    }
                    catch
                    {
                        // Exception in
                        // Callback-exception-handler. That was the
                        // app's last chance. Swallow the exception.
                        // FIXME: proper logging
                    }
                }
            }
        }

        public virtual async Task OnFlowControl(FlowControlEventArgs args)
        {
            AsyncEventHandler<FlowControlEventArgs> handler;
            lock (m_eventLock)
            {
                handler = m_flowControl;
            }
            if (handler != null)
            {
                foreach (AsyncEventHandler<FlowControlEventArgs> h in handler.GetInvocationList())
                {
                    try
                    {
                        await h(this, args);
                    }
                    catch (Exception e)
                    {
                        await OnCallbackException(CallbackExceptionEventArgs.Build(e, "OnFlowControl"));
                    }
                }
            }
        }

        ///<summary>Broadcasts notification of the final shutdown of the model.</summary>
        ///<remarks>
        ///<para>
        ///Do not call anywhere other than at the end of OnSessionShutdown.
        ///</para>
        ///<para>
        ///Must not be called when m_closeReason == null, because
        ///otherwise there's a window when a new continuation could be
        ///being enqueued at the same time as we're broadcasting the
        ///shutdown event. See the definition of Enqueue() above.
        ///</para>
        ///</remarks>
        public virtual async Task OnModelShutdown(ShutdownEventArgs reason)
        {
            m_continuationQueue.HandleModelShutdown(reason);
            AsyncEventHandler<ShutdownEventArgs> handler;
            lock (m_shutdownLock)
            {
                handler = m_modelShutdown;
                m_modelShutdown = null;
            }
            if (handler != null)
            {
                foreach (AsyncEventHandler<ShutdownEventArgs> h in handler.GetInvocationList())
                {
                    try
                    {
                        await h(this, reason);
                    }
                    catch (Exception e)
                    {
                        await OnCallbackException(CallbackExceptionEventArgs.Build(e, "OnModelShutdown"));
                    }
                }
            }
            lock (m_unconfirmedSet.SyncRoot)
                Monitor.Pulse(m_unconfirmedSet.SyncRoot);
            m_flowControlBlock.Set();
        }

        public async Task OnSessionShutdown(object sender, ShutdownEventArgs reason)
        {
            this.ConsumerDispatcher.Quiesce();
            SetCloseReason(reason);
            await OnModelShutdown(reason);
            BroadcastShutdownToConsumers(m_consumers, reason);
            await this.ConsumerDispatcher.Shutdown(this);
        }

        protected void BroadcastShutdownToConsumers(IDictionary<string, IBasicConsumer> cs, ShutdownEventArgs reason)
        {
            foreach (var c in cs)
            {
                this.ConsumerDispatcher.HandleModelShutdown(c.Value, reason);
            }
        }

        public bool SetCloseReason(ShutdownEventArgs reason)
        {
            if (CloseReason == null)
            {
                lock (m_shutdownLock)
                {
                    if (CloseReason == null)
                    {
                        CloseReason = reason;
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
            }
            else
                return false;
        }

        public override string ToString()
        {
            return Session.ToString();
        }

        public Task TransmitAndEnqueue(Command cmd, IRpcContinuation k)
        {
            Enqueue(k);
            return Session.Transmit(cmd);
        }

        public Task Dispose()
        {
            return Abort();
        }

        public abstract Task ConnectionTuneOk(ushort channelMax,
            uint frameMax,
            ushort heartbeat);

        public Task HandleBasicAck(ulong deliveryTag,
            bool multiple)
        {
            var e = new BasicAckEventArgs
            {
                DeliveryTag = deliveryTag,
                Multiple = multiple
            };
            return OnBasicAck(e);
        }

        public void HandleBasicCancel(string consumerTag, bool nowait)
        {
            IBasicConsumer consumer;
            lock (m_consumers)
            {
                consumer = m_consumers[consumerTag];
                m_consumers.Remove(consumerTag);
            }
            if (consumer == null)
            {
                consumer = DefaultConsumer;
            }
            ConsumerDispatcher.HandleBasicCancel(consumer, consumerTag);
        }

        public void HandleBasicCancelOk(string consumerTag)
        {
            var k =
                (BasicConsumerRpcContinuation)m_continuationQueue.Next();
/*
            Trace.Assert(k.m_consumerTag == consumerTag, string.Format(
                "Consumer tag mismatch during cancel: {0} != {1}",
                k.m_consumerTag,
                consumerTag
                ));
*/
            lock (m_consumers)
            {
                k.m_consumer = m_consumers[consumerTag];
                m_consumers.Remove(consumerTag);
            }
            ConsumerDispatcher.HandleBasicCancelOk(k.m_consumer, consumerTag);
            k.HandleCommand(null); // release the continuation.
        }

        public void HandleBasicConsumeOk(string consumerTag)
        {
            var k =
                (BasicConsumerRpcContinuation)m_continuationQueue.Next();
            k.m_consumerTag = consumerTag;
            lock (m_consumers)
            {
                m_consumers[consumerTag] = k.m_consumer;
            }
            ConsumerDispatcher.HandleBasicConsumeOk(k.m_consumer, consumerTag);
            k.HandleCommand(null); // release the continuation.
        }

        public virtual void HandleBasicDeliver(string consumerTag,
            ulong deliveryTag,
            bool redelivered,
            string exchange,
            string routingKey,
            IBasicProperties basicProperties,
            byte[] body)
        {
            IBasicConsumer consumer;
            lock (m_consumers)
            {
                consumer = m_consumers[consumerTag];
            }
            if (consumer == null)
            {
                if (DefaultConsumer == null)
                {
                    throw new InvalidOperationException("Unsolicited delivery -" +
                                                        " see IModel.DefaultConsumer to handle this" +
                                                        " case.");
                }
                else
                {
                    consumer = DefaultConsumer;
                }
            }

            ConsumerDispatcher.HandleBasicDeliver(consumer,
                    consumerTag,
                    deliveryTag,
                    redelivered,
                    exchange,
                    routingKey,
                    basicProperties,
                    body);
        }

        public void HandleBasicGetEmpty()
        {
            var k = (BasicGetRpcContinuation)m_continuationQueue.Next();
            k.m_result = null;
            k.HandleCommand(null); // release the continuation.
        }

        public virtual void HandleBasicGetOk(ulong deliveryTag,
            bool redelivered,
            string exchange,
            string routingKey,
            uint messageCount,
            IBasicProperties basicProperties,
            byte[] body)
        {
            var k = (BasicGetRpcContinuation)m_continuationQueue.Next();
            k.m_result = new BasicGetResult(deliveryTag,
                redelivered,
                exchange,
                routingKey,
                messageCount,
                basicProperties,
                body);
            k.HandleCommand(null); // release the continuation.
        }

        public async Task HandleBasicNack(ulong deliveryTag,
            bool multiple,
            bool requeue)
        {
            var e = new BasicNackEventArgs();
            e.DeliveryTag = deliveryTag;
            e.Multiple = multiple;
            e.Requeue = requeue;
            await OnBasicNack(e);
        }

        public async Task HandleBasicRecoverOk()
        {
            var k = (SimpleBlockingRpcContinuation)m_continuationQueue.Next();
            await OnBasicRecoverOk(new EventArgs());
            k.HandleCommand(null);
        }

        public async Task HandleBasicReturn(ushort replyCode,
            string replyText,
            string exchange,
            string routingKey,
            IBasicProperties basicProperties,
            byte[] body)
        {
            var e = new BasicReturnEventArgs();
            e.ReplyCode = replyCode;
            e.ReplyText = replyText;
            e.Exchange = exchange;
            e.RoutingKey = routingKey;
            e.BasicProperties = basicProperties;
            e.Body = body;
            await OnBasicReturn(e);
        }

        public async Task HandleChannelClose(ushort replyCode,
            string replyText,
            ushort classId,
            ushort methodId)
        {
            SetCloseReason(new ShutdownEventArgs(ShutdownInitiator.Peer,
                replyCode,
                replyText,
                classId,
                methodId));

            await Session.Close(CloseReason, false);
            try
            {
                await _Private_ChannelCloseOk();
            }
            finally
            {
                await Session.Notify();
            }
        }

        public Task HandleChannelCloseOk()
        {
            return FinishClose();
        }

        public async Task HandleChannelFlow(bool active)
        {
            if (active)
            {
                m_flowControlBlock.Set();
            }
            else
            {
                m_flowControlBlock.Reset();
            }

            await _Private_ChannelFlowOk(active);

            await OnFlowControl(new FlowControlEventArgs(active));
        }

        public async Task HandleConnectionBlocked(string reason)
        {
            var cb = ((Connection)Session.Connection);

            await cb.HandleConnectionBlocked(reason);
        }

        public async Task HandleConnectionClose(ushort replyCode,
            string replyText,
            ushort classId,
            ushort methodId)
        {
            var reason = new ShutdownEventArgs(ShutdownInitiator.Peer,
                replyCode,
                replyText,
                classId,
                methodId);
            try
            {
                await ((Connection)Session.Connection).InternalClose(reason);
                await _Private_ConnectionCloseOk();
                SetCloseReason((Session.Connection).CloseReason);
            }
            catch (IOException)
            {
                // Ignored. We're only trying to be polite by sending
                // the close-ok, after all.
            }
            catch (AlreadyClosedException)
            {
                // Ignored. We're only trying to be polite by sending
                // the close-ok, after all.
            }
        }

        public void HandleConnectionOpenOk(string knownHosts)
        {
            var k = (ConnectionOpenContinuation)m_continuationQueue.Next();
            k.m_redirect = false;
            k.m_host = null;
            k.m_knownHosts = knownHosts;
            k.HandleCommand(null); // release the continuation.
        }

        public void HandleConnectionSecure(byte[] challenge)
        {
            var k = (ConnectionStartRpcContinuation)m_continuationQueue.Next();
            k.m_result = new ConnectionSecureOrTune
            {
                m_challenge = challenge
            };
            k.HandleCommand(null); // release the continuation.
        }

        public void HandleConnectionStart(byte versionMajor,
            byte versionMinor,
            IDictionary<string, object> serverProperties,
            byte[] mechanisms,
            byte[] locales)
        {
            if (m_connectionStartCell == null)
            {
                var reason =
                    new ShutdownEventArgs(ShutdownInitiator.Library,
                        Constants.CommandInvalid,
                        "Unexpected Connection.Start");
                ((Connection)Session.Connection).Close(reason);
            }
            var details = new ConnectionStartDetails
            {
                m_versionMajor = versionMajor,
                m_versionMinor = versionMinor,
                m_serverProperties = serverProperties,
                m_mechanisms = mechanisms,
                m_locales = locales
            };
            m_connectionStartCell.TrySetResult(details);
            m_connectionStartCell = null;
        }

        ///<summary>Handle incoming Connection.Tune
        ///methods.</summary>
        public void HandleConnectionTune(ushort channelMax,
            uint frameMax,
            ushort heartbeat)
        {
            var k = (ConnectionStartRpcContinuation)m_continuationQueue.Next();
            k.m_result = new ConnectionSecureOrTune
            {
                m_tuneDetails =
                {
                    m_channelMax = channelMax,
                    m_frameMax = frameMax,
                    m_heartbeat = heartbeat
                }
            };
            k.HandleCommand(null); // release the continuation.
        }

        public async Task HandleConnectionUnblocked()
        {
            var cb = ((Connection)Session.Connection);

            await cb.HandleConnectionUnblocked();
        }

        public void HandleQueueDeclareOk(string queue,
            uint messageCount,
            uint consumerCount)
        {
            var k = (QueueDeclareRpcContinuation)m_continuationQueue.Next();
            k.m_result = new QueueDeclareOk(queue, messageCount, consumerCount);
            k.HandleCommand(null); // release the continuation.
        }

        public abstract Task _Private_BasicCancel(string consumerTag,
            bool nowait);

        public abstract Task _Private_BasicConsume(string queue,
            string consumerTag,
            bool noLocal,
            bool autoAck,
            bool exclusive,
            bool nowait,
            IDictionary<string, object> arguments);

        public abstract Task _Private_BasicGet(string queue,
            bool autoAck);

        public abstract Task _Private_BasicPublish(string exchange,
            string routingKey,
            bool mandatory,
            IBasicProperties basicProperties,
            byte[] body);

        public abstract Task _Private_BasicRecover(bool requeue);

        public abstract Task _Private_ChannelClose(ushort replyCode,
            string replyText,
            ushort classId,
            ushort methodId);

        public abstract Task _Private_ChannelCloseOk();

        public abstract Task _Private_ChannelFlowOk(bool active);

        public abstract Task _Private_ChannelOpen(string outOfBand);

        public abstract Task _Private_ConfirmSelect(bool nowait);

        public abstract Task _Private_ConnectionClose(ushort replyCode,
            string replyText,
            ushort classId,
            ushort methodId);

        public abstract Task _Private_ConnectionCloseOk();

        public abstract Task _Private_ConnectionOpen(string virtualHost,
            string capabilities,
            bool insist);

        public abstract Task _Private_ConnectionSecureOk(byte[] response);

        public abstract Task _Private_ConnectionStartOk(IDictionary<string, object> clientProperties,
            string mechanism,
            byte[] response,
            string locale);

        public abstract Task _Private_ExchangeBind(string destination,
            string source,
            string routingKey,
            bool nowait,
            IDictionary<string, object> arguments);

        public abstract Task _Private_ExchangeDeclare(string exchange,
            string type,
            bool passive,
            bool durable,
            bool autoDelete,
            bool @internal,
            bool nowait,
            IDictionary<string, object> arguments);

        public abstract Task _Private_ExchangeDelete(string exchange,
            bool ifUnused,
            bool nowait);

        public abstract Task _Private_ExchangeUnbind(string destination,
            string source,
            string routingKey,
            bool nowait,
            IDictionary<string, object> arguments);

        public abstract Task _Private_QueueBind(string queue,
            string exchange,
            string routingKey,
            bool nowait,
            IDictionary<string, object> arguments);

        public abstract Task _Private_QueueDeclare(string queue,
            bool passive,
            bool durable,
            bool exclusive,
            bool autoDelete,
            bool nowait,
            IDictionary<string, object> arguments);

        public abstract Task<uint> _Private_QueueDelete(string queue,
            bool ifUnused,
            bool ifEmpty,
            bool nowait);

        public abstract Task<uint> _Private_QueuePurge(string queue,
            bool nowait);

        public Task Abort()
        {
            return Abort(Constants.ReplySuccess, "Goodbye");
        }

        public Task Abort(ushort replyCode, string replyText)
        {
            return Close(replyCode, replyText, true);
        }

        public abstract Task BasicAck(ulong deliveryTag, bool multiple);

        public async Task BasicCancel(string consumerTag)
        {
            var k = new BasicConsumerRpcContinuation { m_consumerTag = consumerTag };

            Enqueue(k);

            await _Private_BasicCancel(consumerTag, false);
            await k.GetReply(this.ContinuationTimeout);
            lock (m_consumers)
            {
                m_consumers.Remove(consumerTag);
            }

            ModelShutdown -= k.m_consumer.HandleModelShutdown;
        }

        public async Task<string> BasicConsume(string queue,
            bool autoAck,
            string consumerTag,
            bool noLocal,
            bool exclusive,
            IDictionary<string, object> arguments,
            IBasicConsumer consumer)
        {
            // TODO: Replace with flag
            var asyncDispatcher = ConsumerDispatcher as AsyncConsumerDispatcher;
            if (asyncDispatcher != null)
            {
                var asyncConsumer = consumer as IBasicConsumer;
                if (asyncConsumer == null)
                {
                    // TODO: Friendly message
                    throw new InvalidOperationException("In the async mode you have to use an async consumer");
                }
            }

            var k = new BasicConsumerRpcContinuation { m_consumer = consumer };

            Enqueue(k);
            // Non-nowait. We have an unconventional means of getting
            // the RPC response, but a response is still expected.
            await _Private_BasicConsume(queue, consumerTag, noLocal, autoAck, exclusive,
                /*nowait:*/ false, arguments);
            await k.GetReply(this.ContinuationTimeout);
            string actualConsumerTag = k.m_consumerTag;

            return actualConsumerTag;
        }

        public async Task<BasicGetResult> BasicGet(string queue,
            bool autoAck)
        {
            var k = new BasicGetRpcContinuation();
            Enqueue(k);
            await _Private_BasicGet(queue, autoAck);
            await k.GetReply(this.ContinuationTimeout);
            return k.m_result;
        }

        public abstract Task BasicNack(ulong deliveryTag,
            bool multiple,
            bool requeue);

        internal void AllocatatePublishSeqNos(int count)
        {
            var c = 0;
            lock (m_unconfirmedSet.SyncRoot)
            {
                while(c < count)
                {
                    if (NextPublishSeqNo > 0)
                    {
                            if (!m_unconfirmedSet.Contains(NextPublishSeqNo))
                            {
                                m_unconfirmedSet.Add(NextPublishSeqNo);
                            }
                            NextPublishSeqNo++;
                    }
                    c++;
                }
            }
        }

        public Task BasicPublish(string exchange,
            string routingKey,
            bool mandatory,
            IBasicProperties basicProperties,
            byte[] body)
        {
            if (basicProperties == null)
            {
                basicProperties = CreateBasicProperties();
            }
            if (NextPublishSeqNo > 0)
            {
                lock (m_unconfirmedSet.SyncRoot)
                {
                    if (!m_unconfirmedSet.Contains(NextPublishSeqNo))
                    {
                        m_unconfirmedSet.Add(NextPublishSeqNo);
                    }
                    NextPublishSeqNo++;
                }
            }
            return _Private_BasicPublish(exchange,
                routingKey,
                mandatory,
                basicProperties,
                body);
        }

        public abstract Task BasicQos(uint prefetchSize,
            ushort prefetchCount,
            bool global);

        public async Task BasicRecover(bool requeue)
        {
            var k = new SimpleBlockingRpcContinuation();

            Enqueue(k);
            await _Private_BasicRecover(requeue);
            await k.GetReply(this.ContinuationTimeout);
        }

        public abstract Task BasicRecoverAsync(bool requeue);

        public abstract Task BasicReject(ulong deliveryTag,
            bool requeue);

        public void Close()
        {
            Close(Constants.ReplySuccess, "Goodbye");
        }

        public void Close(ushort replyCode, string replyText)
        {
            Close(replyCode, replyText, false);
        }

        public void ConfirmSelect()
        {
            if (NextPublishSeqNo == 0UL)
            {
                NextPublishSeqNo = 1;
            }
            _Private_ConfirmSelect(false);
        }

        ///////////////////////////////////////////////////////////////////////////

        public abstract IBasicProperties CreateBasicProperties();
        public IBasicPublishBatch CreateBasicPublishBatch()
        {
            return new BasicPublishBatch(this);
        }


        public Task ExchangeBind(string destination,
            string source,
            string routingKey,
            IDictionary<string, object> arguments)
        {
            return _Private_ExchangeBind(destination, source, routingKey, false, arguments);
        }

        public Task ExchangeBindNoWait(string destination,
            string source,
            string routingKey,
            IDictionary<string, object> arguments)
        {
            return _Private_ExchangeBind(destination, source, routingKey, true, arguments);
        }

        public Task ExchangeDeclare(string exchange, string type, bool durable, bool autoDelete, IDictionary<string, object> arguments)
        {
            return _Private_ExchangeDeclare(exchange, type, false, durable, autoDelete, false, false, arguments);
        }

        public Task ExchangeDeclareNoWait(string exchange,
            string type,
            bool durable,
            bool autoDelete,
            IDictionary<string, object> arguments)
        {
            return _Private_ExchangeDeclare(exchange, type, false, durable, autoDelete, false, true, arguments);
        }

        public Task ExchangeDeclarePassive(string exchange)
        {
            return _Private_ExchangeDeclare(exchange, "", true, false, false, false, false, null);
        }

        public Task ExchangeDelete(string exchange,
            bool ifUnused)
        {
            return _Private_ExchangeDelete(exchange, ifUnused, false);
        }

        public Task ExchangeDeleteNoWait(string exchange,
            bool ifUnused)
        {
            return _Private_ExchangeDelete(exchange, ifUnused, false);
        }

        public Task ExchangeUnbind(string destination,
            string source,
            string routingKey,
            IDictionary<string, object> arguments)
        {
            return _Private_ExchangeUnbind(destination, source, routingKey, false, arguments);
        }

        public Task ExchangeUnbindNoWait(string destination,
            string source,
            string routingKey,
            IDictionary<string, object> arguments)
        {
            return _Private_ExchangeUnbind(destination, source, routingKey, true, arguments);
        }

        public Task QueueBind(string queue,
            string exchange,
            string routingKey,
            IDictionary<string, object> arguments)
        {
            return _Private_QueueBind(queue, exchange, routingKey, false, arguments);
        }

        public Task QueueBindNoWait(string queue,
            string exchange,
            string routingKey,
            IDictionary<string, object> arguments)
        {
            return _Private_QueueBind(queue, exchange, routingKey, true, arguments);
        }

        public Task<QueueDeclareOk> QueueDeclare(string queue, bool durable,
                                           bool exclusive, bool autoDelete,
                                           IDictionary<string, object> arguments)
        {
            return QueueDeclare(queue, false, durable, exclusive, autoDelete, arguments);
        }

        public Task QueueDeclareNoWait(string queue, bool durable, bool exclusive,
            bool autoDelete, IDictionary<string, object> arguments)
        {
            return _Private_QueueDeclare(queue, false, durable, exclusive, autoDelete, true, arguments);
        }

        public Task<QueueDeclareOk> QueueDeclarePassive(string queue)
        {
            return QueueDeclare(queue, true, false, false, false, null);
        }

        public async Task<uint> MessageCount(string queue)
        {
            var ok = await QueueDeclarePassive(queue);
            return ok.MessageCount;
        }

        public async Task<uint> ConsumerCount(string queue)
        {
            var ok = await QueueDeclarePassive(queue);
            return ok.ConsumerCount;
        }

        public Task<uint> QueueDelete(string queue,
            bool ifUnused,
            bool ifEmpty)
        {
            return _Private_QueueDelete(queue, ifUnused, ifEmpty, false);
        }

        public Task QueueDeleteNoWait(string queue,
            bool ifUnused,
            bool ifEmpty)
        {
            return _Private_QueueDelete(queue, ifUnused, ifEmpty, true);
        }

        public Task<uint> QueuePurge(string queue)
        {
            return _Private_QueuePurge(queue, false);
        }

        public abstract Task QueueUnbind(string queue,
            string exchange,
            string routingKey,
            IDictionary<string, object> arguments);

        public abstract Task TxCommit();

        public abstract Task TxRollback();

        public abstract Task TxSelect();

        public bool WaitForConfirms(TimeSpan timeout, out bool timedOut)
        {
            if (NextPublishSeqNo == 0UL)
            {
                throw new InvalidOperationException("Confirms not selected");
            }
            bool isWaitInfinite = (timeout.TotalMilliseconds == Timeout.Infinite);
            Stopwatch stopwatch = Stopwatch.StartNew();
            lock (m_unconfirmedSet.SyncRoot)
            {
                while (true)
                {
                    if (!IsOpen)
                    {
                        throw new AlreadyClosedException(CloseReason);
                    }

                    if (m_unconfirmedSet.Count == 0)
                    {
                        bool aux = m_onlyAcksReceived;
                        m_onlyAcksReceived = true;
                        timedOut = false;
                        return aux;
                    }
                    if (isWaitInfinite)
                    {
                        Monitor.Wait(m_unconfirmedSet.SyncRoot);
                    }
                    else
                    {
                        TimeSpan elapsed = stopwatch.Elapsed;
                        if (elapsed > timeout || !Monitor.Wait(
                            m_unconfirmedSet.SyncRoot, timeout - elapsed))
                        {
                            timedOut = true;
                            return true;
                        }
                    }
                }
            }
        }

        public bool WaitForConfirms()
        {
            bool timedOut;
            return WaitForConfirms(TimeSpan.FromMilliseconds(Timeout.Infinite), out timedOut);
        }

        public bool WaitForConfirms(TimeSpan timeout)
        {
            bool timedOut;
            return WaitForConfirms(timeout, out timedOut);
        }

        public Task WaitForConfirmsOrDie()
        {
            return WaitForConfirmsOrDie(TimeSpan.FromMilliseconds(Timeout.Infinite));
        }

        public async Task WaitForConfirmsOrDie(TimeSpan timeout)
        {
            bool timedOut;
            bool onlyAcksReceived = WaitForConfirms(timeout, out timedOut);
            if (!onlyAcksReceived)
            {
                await Close(new ShutdownEventArgs(ShutdownInitiator.Application,
                    Constants.ReplySuccess,
                    "Nacks Received", new IOException("nack received")),
                    false);
                throw new IOException("Nacks Received");
            }
            if (timedOut)
            {
                await Close(new ShutdownEventArgs(ShutdownInitiator.Application,
                    Constants.ReplySuccess,
                    "Timed out waiting for acks",
                    new IOException("timed out waiting for acks")),
                    false);
                throw new IOException("Timed out waiting for acks");
            }
        }

        internal void SendCommands(IList<Command> commands)
        {
            m_flowControlBlock.WaitOne();
            AllocatatePublishSeqNos(commands.Count);
            Session.Transmit(commands);
        }

        protected virtual void handleAckNack(ulong deliveryTag, bool multiple, bool isNack)
        {
            lock (m_unconfirmedSet.SyncRoot)
            {
                if (multiple)
                {
                    for (ulong i = m_unconfirmedSet[0]; i <= deliveryTag; i++)
                    {
                        // removes potential duplicates
                        while (m_unconfirmedSet.Remove(i))
                        {
                        }
                    }
                }
                else
                {
                    while (m_unconfirmedSet.Remove(deliveryTag))
                    {
                    }
                }
                m_onlyAcksReceived = m_onlyAcksReceived && !isNack;
                if (m_unconfirmedSet.Count == 0)
                {
                    Monitor.Pulse(m_unconfirmedSet.SyncRoot);
                }
            }
        }

        private async Task<QueueDeclareOk> QueueDeclare(string queue, bool passive, bool durable, bool exclusive,
            bool autoDelete, IDictionary<string, object> arguments)
        {
            var k = new QueueDeclareRpcContinuation();
            Enqueue(k);
            await _Private_QueueDeclare(queue, passive, durable, exclusive, autoDelete, false, arguments);
            await k.GetReply(this.ContinuationTimeout);
            return k.m_result;
        }


        public class BasicConsumerRpcContinuation : SimpleBlockingRpcContinuation
        {
            public IBasicConsumer m_consumer;
            public string m_consumerTag;
        }

        public class BasicGetRpcContinuation : SimpleBlockingRpcContinuation
        {
            public BasicGetResult m_result;
        }

        public class ConnectionOpenContinuation : SimpleBlockingRpcContinuation
        {
            public string m_host;
            public string m_knownHosts;
            public bool m_redirect;
        }

        public class ConnectionStartRpcContinuation : SimpleBlockingRpcContinuation
        {
            public ConnectionSecureOrTune m_result;
        }

        public class QueueDeclareRpcContinuation : SimpleBlockingRpcContinuation
        {
            public QueueDeclareOk m_result;
        }
    }
}
