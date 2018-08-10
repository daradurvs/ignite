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
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import static org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType.BYTE_ARR;
import static org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType.INT;
import static org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType.STRING;

/**
 * Services single node map message.
 */
public class ServicesSingleMapMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Local services assignments. */
    @GridToStringInclude
    private Map<String, Integer> assigns;

    /** Deployment errors. */
    @GridToStringInclude
    private Map<String, byte[]> errors;

    /** Sender id. */
    private UUID snd;

    /** Exchange id. */
    private ServicesDeploymentExchangeId exchId;

    /**
     * Empty constructor for marshalling purposes.
     */
    public ServicesSingleMapMessage() {
    }

    /**
     * @param snd Sender id.
     * @param exchId Exchange id.
     * @param assigns Local services assignments.
     */
    public ServicesSingleMapMessage(UUID snd, ServicesDeploymentExchangeId exchId,
        Map<String, Integer> assigns) {
        this.snd = snd;
        this.exchId = exchId;
        this.assigns = assigns;
        this.errors = Collections.emptyMap();
    }

    /**
     * @return Sender id.
     */
    public UUID senderId() {
        return snd;
    }

    /**
     * @return Single node services assignments.
     */
    public Map<String, Integer> assigns() {
        return assigns;
    }

    /**
     * @param assigns Single node services assignments.
     */
    public void assigns(Map<String, Integer> assigns) {
        this.assigns = assigns;
    }

    /**
     * @return Deployment errors.
     */
    public Map<String, byte[]> errors() {
        return errors;
    }

    /**
     * @param errors Deployment errors.
     */
    public void errors(Map<String, byte[]> errors) {
        this.errors = errors;
    }

    /**
     * @return Exchange id.
     */
    public ServicesDeploymentExchangeId exchangeId() {
        return exchId;
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
                if (!writer.writeMap("assigns", assigns, STRING, INT))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMap("errors", errors, STRING, BYTE_ARR))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeUuid("snd", snd))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeMessage("exchId", exchId))
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
                assigns = reader.readMap("assigns", STRING, INT, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                errors = reader.readMap("errors", STRING, BYTE_ARR, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                snd = reader.readUuid("snd");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                exchId = reader.readMessage("exchId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(ServicesSingleMapMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 136;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServicesSingleMapMessage.class, this);
    }
}
