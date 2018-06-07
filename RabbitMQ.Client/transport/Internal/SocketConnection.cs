// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Diagnostics;
using System.IO;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Client.Transport.Internal
{

    internal sealed class SocketConnection
    {
        private static readonly int MinAllocBufferSize = 2048;

        private readonly Socket _socket;
        private static readonly PipeScheduler Scheduler = new IOQueue();
        private readonly ISocketsTrace _trace;
        private readonly SocketReceiver _receiver;
        private readonly SocketSender _sender;
        private readonly CancellationTokenSource _connectionClosedTokenSource = new CancellationTokenSource();
        private readonly Pipe _pipe;

        private readonly object _shutdownLock = new object();
        private volatile bool _aborted;
        private long _totalBytesWritten;

        internal SocketConnection(Socket socket)
        {
            Debug.Assert(socket != null);

            _pipe = new Pipe();
            _socket = socket;
            _trace = new EmptySocketsTrace();

            var localEndPoint = (IPEndPoint)_socket.LocalEndPoint;
            var remoteEndPoint = (IPEndPoint)_socket.RemoteEndPoint;

            LocalAddress = localEndPoint.Address;
            LocalPort = localEndPoint.Port;

            RemoteAddress = remoteEndPoint.Address;
            RemotePort = remoteEndPoint.Port;

            ConnectionClosed = _connectionClosedTokenSource.Token;

            var pair = DuplexPipe.CreateConnectionPair();
            
            Transport = pair.Transport;          
            Application = pair.Application;

            _receiver = new SocketReceiver(_socket, Scheduler);
            _sender = new SocketSender(_socket, Scheduler);
        }


        public bool Connected
        {
            get
            {
                if (_socket == null) return false;
                return _socket.Connected;
            }
        }

        public int SendTimeout
        {
            get
            {
                AssertSocket();
                return _socket.SendTimeout;
            }
            set
            {
                AssertSocket();
                _socket.SendTimeout = value;
            }
        }

        public int ReceiveTimeout
        {
            get
            {
                AssertSocket();
                return _socket.ReceiveTimeout;
            }
            set
            {
                AssertSocket();
                _socket.ReceiveTimeout = value;
            }
        }

        private void AssertSocket()
        {
            if (_socket == null)
            {
                throw new InvalidOperationException("Cannot perform operation as socket is null");
            }
        }

        public IPAddress RemoteAddress { get; set; }

        public int RemotePort { get; set; }

        public IPAddress LocalAddress { get; set; }

        public int LocalPort { get; set; }

        public PipeWriter Input => Application.Output;

        public PipeReader Output => Application.Input;

        public CancellationToken ConnectionClosed { get; set; }

        public string ConnectionId { get; set; }

        public IDuplexPipe Transport { get; set; }

        public IDuplexPipe Application { get; set; }

        public async Task StartAsync()
        {
            try
            {
                // Spawn send and receive logic
                var receiveTask = DoReceive();
                var sendTask = DoSend();

                // Now wait for both to complete
                await receiveTask;
                await sendTask;

                _receiver.Dispose();
                _sender.Dispose();
                ThreadPool.QueueUserWorkItem(state => ((SocketConnection)state).CancelConnectionClosedToken(), this);
            }
            catch (Exception ex)
            {
                _trace.LogError(ex, $"Unexpected exception in {nameof(SocketConnection)}.{nameof(StartAsync)}.");
            }
        }

        public void Abort()
        {
            // Try to gracefully close the socket to match libuv behavior.
            Shutdown();
        }

        private async Task DoReceive()
        {
            Exception error = null;

            try
            {
                await ProcessReceives();
            }
            catch (SocketException ex) when (ex.SocketErrorCode == SocketError.ConnectionReset)
            {
                error = new ConnectionResetException(ex.Message, ex);
                _trace.ConnectionReset(ConnectionId);
            }
            catch (SocketException ex) when (ex.SocketErrorCode == SocketError.OperationAborted ||
                                             ex.SocketErrorCode == SocketError.ConnectionAborted ||
                                             ex.SocketErrorCode == SocketError.Interrupted ||
                                             ex.SocketErrorCode == SocketError.InvalidArgument)
            {
                if (!_aborted)
                {
                    // Calling Dispose after ReceiveAsync can cause an "InvalidArgument" error on *nix.
                    error = new ConnectionAbortedException();
                    _trace.ConnectionError(ConnectionId, error);
                }
            }
            catch (ObjectDisposedException)
            {
                if (!_aborted)
                {
                    error = new ConnectionAbortedException();
                    _trace.ConnectionError(ConnectionId, error);
                }
            }
            catch (IOException ex)
            {
                error = ex;
                _trace.ConnectionError(ConnectionId, error);
            }
            catch (Exception ex)
            {
                error = new IOException(ex.Message, ex);
                _trace.ConnectionError(ConnectionId, error);
            }
            finally
            {
                if (_aborted)
                {
                    error = error ?? new ConnectionAbortedException();
                }

                Input.Complete(error);
            }
        }

        private async Task ProcessReceives()
        {
            while (true)
            {
                // Ensure we have some reasonable amount of buffer space
                var buffer = Input.GetMemory(MinAllocBufferSize);

                var bytesReceived = await _receiver.ReceiveAsync(buffer);

                if (bytesReceived == 0)
                {
                    // FIN
                    _trace.ConnectionReadFin(ConnectionId);
                    break;
                }

                Input.Advance(bytesReceived);

                var flushTask = Input.FlushAsync();

                if (!flushTask.IsCompleted)
                {
                    _trace.ConnectionPause(ConnectionId);

                    await flushTask;

                    _trace.ConnectionResume(ConnectionId);
                }

                var result = flushTask.GetAwaiter().GetResult();
                if (result.IsCompleted)
                {
                    // Pipe consumer is shut down, do we stop writing
                    break;
                }
            }
        }

        private async Task DoSend()
        {
            Exception error = null;

            try
            {
                await ProcessSends();
            }
            catch (SocketException ex) when (ex.SocketErrorCode == SocketError.OperationAborted)
            {
                error = null;
            }
            catch (ObjectDisposedException)
            {
                error = null;
            }
            catch (IOException ex)
            {
                error = ex;
            }
            catch (Exception ex)
            {
                error = new IOException(ex.Message, ex);
            }
            finally
            {
                Shutdown();

                // Complete the output after disposing the socket
                Output.Complete(error);
            }
        }

        private async Task ProcessSends()
        {
            while (true)
            {
                // Wait for data to write from the pipe producer
                var result = await Output.ReadAsync();
                var buffer = result.Buffer;

                if (result.IsCanceled)
                {
                    break;
                }

                var end = buffer.End;
                var isCompleted = result.IsCompleted;
                if (!buffer.IsEmpty)
                {
                    await _sender.SendAsync(buffer);
                }

                // This is not interlocked because there could be a concurrent writer.
                // Instead it's to prevent read tearing on 32-bit systems.
                Interlocked.Add(ref _totalBytesWritten, buffer.Length);

                Output.AdvanceTo(end);

                if (isCompleted)
                {
                    break;
                }
            }
        }

        private void Shutdown()
        {
            lock (_shutdownLock)
            {
                if (!_aborted)
                {
                    // Make sure to close the connection only after the _aborted flag is set.
                    // Without this, the RequestsCanBeAbortedMidRead test will sometimes fail when
                    // a BadHttpRequestException is thrown instead of a TaskCanceledException.
                    _aborted = true;
                    _trace.ConnectionWriteFin(ConnectionId);

                    try
                    {
                        // Try to gracefully close the socket even for aborts to match libuv behavior.
                        _socket.Shutdown(SocketShutdown.Both);
                    }
                    catch
                    {
                        // Ignore any errors from Socket.Shutdown since we're tearing down the connection anyway.
                    }

                    _socket.Dispose();
                }
            }
        }

        private void CancelConnectionClosedToken()
        {
            try
            {
                _connectionClosedTokenSource.Cancel();
                _connectionClosedTokenSource.Dispose();
            }
            catch (Exception ex)
            {
                _trace.LogError(ex, $"Unexpected exception in {nameof(SocketConnection)}.{nameof(CancelConnectionClosedToken)}.");
            }
        }
    }

    public class ConnectionAbortedException : OperationCanceledException
    {
        public ConnectionAbortedException() :
            this("The connection was aborted")
        {

        }

        public ConnectionAbortedException(string message) : base(message)
        {
        }

        public ConnectionAbortedException(string message, Exception inner) : base(message, inner)
        {
        }
    }

    public class ConnectionResetException : IOException
    {
        public ConnectionResetException(string message) : base(message)
        {
        }

        public ConnectionResetException(string message, Exception inner) : base(message, inner)
        {
        }
    }

    internal class DuplexPipe : IDuplexPipe
    {
        public DuplexPipe(PipeReader reader, PipeWriter writer)
        {
            Input = reader;
            Output = writer;
        }

        public PipeReader Input { get; }

        public PipeWriter Output { get; }

        public static DuplexPipePair CreateConnectionPair()
        {
            var input = new Pipe();
            var output = new Pipe();

            var transportToApplication = new DuplexPipe(output.Reader, input.Writer);
            var applicationToTransport = new DuplexPipe(input.Reader, output.Writer);

            return new DuplexPipePair(applicationToTransport, transportToApplication);
        }

        // This class exists to work around issues with value tuple on .NET Framework
        public struct DuplexPipePair
        {
            public IDuplexPipe Transport { get; }

            public IDuplexPipe Application { get; }

            public DuplexPipePair(IDuplexPipe transport, IDuplexPipe application)
            {
                Transport = transport;
                Application = application;
            }
        }
    }
}
