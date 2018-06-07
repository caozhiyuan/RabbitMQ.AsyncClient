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

#if !NETFX_CORE
using RabbitMQ.Client.Exceptions;
using RabbitMQ.Util;
using System;
using System.IO;
using System.Net;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client.Transport.Internal;

namespace RabbitMQ.Client.Impl
{
    static class TaskExtensions
    {
        public static Task CompletedTask = Task.FromResult(0);

        public static async Task TimeoutAfter(this Task task, int millisecondsTimeout)
        {
            if (task == await Task.WhenAny(task, Task.Delay(millisecondsTimeout)).ConfigureAwait(false))
                await task;
            else
                throw new TimeoutException();
        }
    }

    public class SocketFrameHandler : IFrameHandler
    {
        private SocketConnection socketConnection;
        private Stream m_netstream;
        private readonly object _semaphore = new object();
        private bool _closed;
        private readonly Func<AddressFamily, Socket> m_socketFactory;
        private readonly SemaphoreSlim semaphoreSlim = new SemaphoreSlim(1, 1);
        private readonly int m_connectionTimeout;
        private readonly int m_readTimeout;
        private readonly int m_writeTimeout;

        public SocketFrameHandler(AmqpTcpEndpoint endpoint,
            Func<AddressFamily, Socket> socketFactory,
            int connectionTimeout, 
            int readTimeout, 
            int writeTimeout)
        {
            this.Endpoint = endpoint;
            this.m_socketFactory = socketFactory;
            this.m_connectionTimeout = connectionTimeout;
            this.m_readTimeout = readTimeout;
            this.m_writeTimeout = writeTimeout;
        }

        public async Task Connect()
        {
            Socket m_socket = null;
            var endpoint = this.Endpoint;
            if (ShouldTryIPv6(endpoint))
            {
                try
                {
                    m_socket = await ConnectUsingIPv6(endpoint, m_socketFactory, m_connectionTimeout);
                }
                catch (ConnectFailureException)
                {
                    m_socket = null;
                }
            }

            if (m_socket == null && endpoint.AddressFamily != AddressFamily.InterNetworkV6)
            {
                m_socket = await ConnectUsingIPv4(endpoint, m_socketFactory, m_connectionTimeout);
            }

            socketConnection = new SocketConnection(m_socket)
            {
                ReceiveTimeout = m_readTimeout,
                SendTimeout = m_writeTimeout
            };

            Stream netstream = new RawStream(socketConnection.Transport.Input, socketConnection.Transport.Output);
            if (endpoint.Ssl.Enabled)
            {
                try
                {
                    netstream = await SslHelper.TcpUpgrade(netstream, endpoint.Ssl);
                }
                catch (Exception)
                {
                    Close();
                    throw;
                }
            }

            m_netstream = netstream;

            _ = socketConnection.StartAsync();
        }

        public AmqpTcpEndpoint Endpoint { get; set; }

        public EndPoint LocalEndPoint
        {
            get { return new IPEndPoint(socketConnection.LocalAddress, socketConnection.LocalPort); }
        }

        public int LocalPort
        {
            get { return socketConnection.LocalPort; }
        }

        public EndPoint RemoteEndPoint
        {
            get { return new IPEndPoint(socketConnection.RemoteAddress, socketConnection.RemotePort); }
        }

        public int RemotePort
        {
            get { return socketConnection.RemotePort; }
        }

        public int ReadTimeout
        {
            set
            {
                try
                {
                    if (socketConnection.Connected)
                    {
                        socketConnection.ReceiveTimeout = value;
                    }
                }
                catch (SocketException)
                {
                    // means that the socket is already closed
                }
            }
        }

        public int WriteTimeout
        {
            set
            {
                socketConnection.SendTimeout = value;
            }
        }

        public void Close()
        {
            lock (_semaphore)
            {
                if (!_closed)
                {
                    try
                    {
                        socketConnection.Abort();
                    }
                    catch (Exception)
                    {
                        // ignore, we are closing anyway
                    }
                    finally
                    {
                        _closed = true;
                    }
                }
            }
        }

        public Task<InboundFrame> ReadFrame()
        {
            return InboundFrame.ReadFrom(m_netstream);
        }

        private static readonly byte[] amqp = Encoding.ASCII.GetBytes("AMQP");

        public Task SendHeader()
        {
            using (var ms = new MemoryStream())
            {
                var nbw = new NetworkBinaryWriter(ms);
                nbw.Write(amqp);
                byte one = (byte)1;
                if (Endpoint.Protocol.Revision != 0)
                {
                    nbw.Write((byte)0);
                    nbw.Write((byte)Endpoint.Protocol.MajorVersion);
                    nbw.Write((byte)Endpoint.Protocol.MinorVersion);
                    nbw.Write((byte)Endpoint.Protocol.Revision);
                }
                else
                {
                    nbw.Write(one);
                    nbw.Write(one);
                    nbw.Write((byte)Endpoint.Protocol.MajorVersion);
                    nbw.Write((byte)Endpoint.Protocol.MinorVersion);
                }

                return WriteFrameBuffer(ms.ToArray());
            }
        }

        public Task WriteFrame(OutboundFrame frame)
        {
            using (var ms = new MemoryStream())
            {
                var nbw = new NetworkBinaryWriter(ms);
                frame.WriteTo(nbw);

                return WriteFrameBuffer(ms.ToArray());
            }
        }

        public Task WriteFrameSet(IList<OutboundFrame> frames)
        {
            using (var ms = new MemoryStream())
            {
                var nbw = new NetworkBinaryWriter(ms);
                foreach (var f in frames) f.WriteTo(nbw);

                return WriteFrameBuffer(ms.ToArray());
            }
        }

        private async Task WriteFrameBuffer(byte[] buffer)
        {
            await semaphoreSlim.WaitAsync();
            try
            {
                await m_netstream.WriteAsync(new ReadOnlyMemory<byte>(buffer));
            }
            finally
            {
                semaphoreSlim.Release();
            }
        }

        private bool ShouldTryIPv6(AmqpTcpEndpoint endpoint)
        {
            return (Socket.OSSupportsIPv6 && endpoint.AddressFamily != AddressFamily.InterNetwork);
        }

        private Task<Socket> ConnectUsingIPv6(AmqpTcpEndpoint endpoint,
                                            Func<AddressFamily, Socket> socketFactory,
                                            int timeout)
        {
            return ConnectUsingAddressFamily(endpoint, socketFactory, timeout, AddressFamily.InterNetworkV6);
        }

        private Task<Socket> ConnectUsingIPv4(AmqpTcpEndpoint endpoint,
                                            Func<AddressFamily, Socket> socketFactory,
                                            int timeout)
        {
            return ConnectUsingAddressFamily(endpoint, socketFactory, timeout, AddressFamily.InterNetwork);
        }

        private async Task<Socket> ConnectUsingAddressFamily(AmqpTcpEndpoint endpoint,
                                                    Func<AddressFamily, Socket> socketFactory,
                                                    int timeout, AddressFamily family)
        {
            Socket socket = socketFactory(family);
            try {
                await ConnectOrFail(socket, endpoint, timeout);
                return socket;
            } catch (ConnectFailureException e) {
                socket.Dispose();
                throw e;
            }
        }

        private async Task ConnectOrFail(Socket socket, AmqpTcpEndpoint endpoint, int timeout)
        {
            try
            {
                await socket.ConnectAsync(endpoint.HostName, endpoint.Port)
                    .TimeoutAfter(timeout)
                    .ConfigureAwait(false);
            }
            catch (ArgumentException e)
            {
                throw new ConnectFailureException("Connection failed", e);
            }
            catch (SocketException e)
            {
                throw new ConnectFailureException("Connection failed", e);
            }
            catch (NotSupportedException e)
            {
                throw new ConnectFailureException("Connection failed", e);
            }
            catch (TimeoutException e)
            {
                throw new ConnectFailureException("Connection failed", e);
            }
        }
    }
}
#endif
