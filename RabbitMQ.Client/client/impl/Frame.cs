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

using RabbitMQ.Client.Exceptions;
using RabbitMQ.Client.Framing;
using RabbitMQ.Util;
using System;
using System.IO;
using System.Threading.Tasks;
#if NETFX_CORE
using Windows.Networking.Sockets;
#else

using System.Net.Sockets;

#endif

namespace RabbitMQ.Client.Impl
{
    public class HeaderOutboundFrame : OutboundFrame
    {
        public HeaderOutboundFrame(int channel, ContentHeaderBase header, int bodyLength) : base(FrameType.FrameHeader, channel)
        {
            NetworkBinaryWriter writer = base.GetWriter();

            writer.Write((ushort)header.ProtocolClassId);
            header.WriteTo(writer, (ulong)bodyLength);
        }
    }

    public class BodySegmentOutboundFrame : OutboundFrame
    {
        public BodySegmentOutboundFrame(int channel, byte[] body, int offset, int count) : base(FrameType.FrameBody, channel)
        {
            NetworkBinaryWriter writer = base.GetWriter();

            writer.Write(body, offset, count);
        }
    }

    public class MethodOutboundFrame : OutboundFrame
    {
        public MethodOutboundFrame(int channel, MethodBase method) : base(FrameType.FrameMethod, channel)
        {
            NetworkBinaryWriter writer = base.GetWriter();

            writer.Write((ushort)method.ProtocolClassId);
            writer.Write((ushort)method.ProtocolMethodId);

            var argWriter = new MethodArgumentWriter(writer);

            method.WriteArgumentsTo(argWriter);

            argWriter.Flush();
        }
    }

    public class EmptyOutboundFrame : OutboundFrame
    {
        private static readonly byte[] m_emptyByteArray = new byte[0];

        public EmptyOutboundFrame() : base(FrameType.FrameHeartbeat, 0)
        {
            base.GetWriter().Write(m_emptyByteArray);
        }

        public override string ToString()
        {
            return base.ToString() + string.Format("(type={0}, channel={1}, {2} bytes of payload)",
                Type,
                Channel,
                Payload == null
                    ? "(null)"
                    : Payload.Length.ToString());
        }
    }

    public class OutboundFrame : Frame
    {
        private readonly MemoryStream m_accumulator;
        private readonly NetworkBinaryWriter writer;

        public OutboundFrame(FrameType type, int channel) : base(type, channel)
        {
            m_accumulator = new MemoryStream();
            writer = new NetworkBinaryWriter(m_accumulator);
        }

        public NetworkBinaryWriter GetWriter()
        {
            return writer;
        }

        public override string ToString()
        {
            return base.ToString() + string.Format("(type={0}, channel={1}, {2} bytes of payload)",
                Type,
                Channel,
                Payload == null
                    ? "(null)"
                    : Payload.Length.ToString());
        }

        public void WriteTo(NetworkBinaryWriter writer)
        {
            var payload = m_accumulator.ToArray();

            writer.Write((byte)Type);
            writer.Write((ushort)Channel);
            writer.Write((uint)payload.Length);
            writer.Write(payload);
            writer.Write((byte)Constants.FrameEnd);
        }
    }

    public class InboundFrame : Frame
    {
        private InboundFrame(FrameType type, int channel, byte[] payload) : base(type, channel, payload)
        {
        }

        private static void ProcessProtocolHeader(NetworkBinaryReader reader)
        {
            byte b1 = reader.ReadByte();
            byte b2 = reader.ReadByte();
            byte b3 = reader.ReadByte();
            if (b1 != 'M' || b2 != 'Q' || b3 != 'P')
            {
                throw new MalformedFrameException("Invalid AMQP protocol header from server");
            }

            int transportHigh = reader.ReadByte();
            int transportLow = reader.ReadByte();
            int serverMajor = reader.ReadByte();
            int serverMinor = reader.ReadByte();
            throw new PacketNotRecognizedException(transportHigh,
                transportLow,
                serverMajor,
                serverMinor);
        }

        public static async Task<InboundFrame> ReadFrom(Stream reader)
        {
            byte[] typebuffer = new byte[1];
            await ReadAsync(reader, typebuffer);

            int type = typebuffer[0];

            NetworkBinaryReader headerReader = null;
            try
            {
                if (type == 'A')
                {
                    byte[] readerbuffer = new byte[7];
                    await ReadAsync(reader, readerbuffer); 

                    headerReader = new NetworkBinaryReader(new MemoryStream(readerbuffer));
                    // Probably an AMQP protocol header, otherwise meaningless
                    ProcessProtocolHeader(headerReader);
                }

                byte[] headerbuffer = new byte[6];
                await ReadAsync(reader, headerbuffer);

                headerReader = new NetworkBinaryReader(new MemoryStream(headerbuffer));

                int channel = headerReader.ReadUInt16();
                int payloadSize = headerReader.ReadInt32(); // FIXME - throw exn on unreasonable value

                byte[] payload = new byte[payloadSize];
                await ReadAsync(reader, payload);
                if (payload.Length != payloadSize)
                {
                    // Early EOF.
                    throw new MalformedFrameException("Short frame - expected " +
                                                      payloadSize + " bytes, got " +
                                                      payload.Length + " bytes");
                }

                var frameEndMarkerbuffer = new byte[1];
                await ReadAsync(reader, frameEndMarkerbuffer);
                int frameEndMarker = frameEndMarkerbuffer[0];
                if (frameEndMarker != Constants.FrameEnd)
                {
                    throw new MalformedFrameException("Bad frame end marker: " + frameEndMarker);
                }

                return new InboundFrame((FrameType)type, channel, payload);
            }
            finally
            {
                headerReader?.Dispose();
            }      
        }

        private static async Task ReadAsync(Stream reader, byte[] byteBuffer)
        {
            int size = byteBuffer.Length;
            int index = 0;
            while (index < size)
            {
                int read = await reader.ReadAsync(byteBuffer, index, size - index);
                index += read;
            }
        }

        public NetworkBinaryReader GetReader()
        {
            return new NetworkBinaryReader(new MemoryStream(base.Payload));
        }

        public override string ToString()
        {
            return base.ToString() + string.Format("(type={0}, channel={1}, {2} bytes of payload)",
                base.Type,
                base.Channel,
                base.Payload == null
                    ? "(null)"
                    : base.Payload.Length.ToString());
        }
    }

    public class Frame
    {
        public Frame(FrameType type, int channel)
        {
            Type = type;
            Channel = channel;
            Payload = null;
        }

        public Frame(FrameType type, int channel, byte[] payload)
        {
            Type = type;
            Channel = channel;
            Payload = payload;
        }

        public int Channel { get; private set; }

        public byte[] Payload { get; private set; }

        public FrameType Type { get; private set; }

        public override string ToString()
        {
            return base.ToString() + string.Format("(type={0}, channel={1}, {2} bytes of payload)",
                Type,
                Channel,
                Payload == null
                    ? "(null)"
                    : Payload.Length.ToString());
        }

        public bool IsMethod()
        {
            return this.Type == FrameType.FrameMethod;
        }
        public bool IsHeader()
        {
            return this.Type == FrameType.FrameHeader;
        }
        public bool IsBody()
        {
            return this.Type == FrameType.FrameBody;
        }
        public bool IsHeartbeat()
        {
            return this.Type == FrameType.FrameHeartbeat;
        }
    }

    public enum FrameType : int
    {
        FrameMethod = 1,
        FrameHeader = 2,
        FrameBody = 3,
        FrameHeartbeat = 8,
        FrameEnd = 206,
        FrameMinSize = 4096
    }

}
