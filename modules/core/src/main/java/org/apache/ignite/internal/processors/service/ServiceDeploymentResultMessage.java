/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.service;

import java.nio.ByteBuffer;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class ServiceDeploymentResultMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final byte SENT_TO_INITIATOR = 0b0001;

    /** Flags. */
    private byte flags;

    /** Service name. */
    private String name;

    /** Serialized deployment error. */
    @Nullable private byte[] errBytes;

    /**
     * Default constructor.
     */
    public ServiceDeploymentResultMessage() {
        // No-op.
    }

    /**
     * @param name Service name.
     */
    public ServiceDeploymentResultMessage(String name) {
        this.name = name;
    }

    /**
     * @return
     */
    public void markToInitiator() {
        flags |= SENT_TO_INITIATOR;
    }

    /**
     * @return
     */
    public boolean isToInitiator() {
        return (flags & SENT_TO_INITIATOR) != 0;
    }

    /**
     * @return Service name.
     */
    public String name() {
        return name;
    }

    /**
     * @param name New service name.
     */
    public void name(String name) {
        this.name = name;
    }

    /**
     * @return
     */
    public boolean hasError() {
        return errBytes != null;
    }

    /**
     * @return Serialized deployment error.
     */
    public byte[] errorBytes() {
        return errBytes;
    }

    /**
     * @param errBytes New serialized deployment error.
     */
    public void errorBytes(byte[] errBytes) {
        this.errBytes = errBytes;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeString("name", name))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeByteArray("errBytes", errBytes))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                name = reader.readString("name");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                errBytes = reader.readByteArray("errBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(ServiceDeploymentResultMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 136;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }
}
