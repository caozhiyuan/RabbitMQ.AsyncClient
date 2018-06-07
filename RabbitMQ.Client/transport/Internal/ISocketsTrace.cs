// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;

namespace RabbitMQ.Client.Transport.Internal
{
    public interface ISocketsTrace
    {
        void ConnectionReadFin(string connectionId);

        void ConnectionWriteFin(string connectionId);

        void ConnectionError(string connectionId, Exception ex);

        void ConnectionReset(string connectionId);

        void ConnectionPause(string connectionId);

        void ConnectionResume(string connectionId);

        void LogError(Exception ex, string message);
    }

    public class EmptySocketsTrace : ISocketsTrace
    {
        public void ConnectionReadFin(string connectionId)
        {
            
        }

        public void ConnectionWriteFin(string connectionId)
        {
            
        }

        public void ConnectionError(string connectionId, Exception ex)
        {
           
        }

        public void ConnectionReset(string connectionId)
        {
          
        }

        public void ConnectionPause(string connectionId)
        {
            
        }

        public void ConnectionResume(string connectionId)
        {
            
        }

        public void LogError(Exception ex, string message)
        {
            ESLog.Error(message, ex);
        }
    }
}
