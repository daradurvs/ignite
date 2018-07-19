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
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType.BYTE_ARR;
import static org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType.MSG;
import static org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType.STRING;

/**
 *
 */
public class ServicesFullAssignmentsMessage implements Message, DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Unique custom message ID. */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** Cluster services assignments. */
    private Map<String, ServiceAssignmentsMap> assigns;

    /** Deployment errors. */
    private Map<String, byte[]> errors;

    IgniteUuid exchId;

    UUID snd;

    /**
     * Empty constructor for marshalling purposes.
     */
    public ServicesFullAssignmentsMessage() {
    }

    /**
     * @param assigns Local services assignments.
     */
    public ServicesFullAssignmentsMessage(Map<String, ServiceAssignmentsMap> assigns) {
        this.assigns = assigns;
    }

    /**
     * @return Local services assignments.
     */
    public Map<String, ServiceAssignmentsMap> assigns() {
        return assigns;
    }

    /**
     * @param assigns New local services assignments.
     */
    public void assigns(Map<String, ServiceAssignmentsMap> assigns) {
        this.assigns = assigns;
    }

    /**
     * @return Deployment errors.
     */
    public Map<String, byte[]> errors() {
        return errors;
    }

    /**
     * @param errors New deployment errors.
     */
    public void errors(Map<String, byte[]> errors) {
        this.errors = errors;
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
                if (!writer.writeMap("assigns", assigns, STRING, MSG))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMap("errors", errors, STRING, BYTE_ARR))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeIgniteUuid("exchId", exchId))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeUuid("snd", snd))
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
                assigns = reader.readMap("assigns", STRING, MSG, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                errors = reader.readMap("errors", STRING, BYTE_ARR, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                exchId = reader.readIgniteUuid("exchId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                snd = reader.readUuid("snd");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(ServicesFullAssignmentsMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 140;
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
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        // No-op.
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean stopProcess() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
        DiscoCache discoCache) {
        // No-op.
        return null;
    }
}
