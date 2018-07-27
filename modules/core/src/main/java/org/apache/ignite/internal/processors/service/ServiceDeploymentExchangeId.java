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
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Service deployment exchange id.
 */
public class ServiceDeploymentExchangeId implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Event node id. */
    private UUID nodeId;

    /** Topology version. */
    private long topVer;

    /** Event type. */
    private int evtType;

    /** Requests id. */
    private IgniteUuid reqId;

    /**
     * Empty constructor for marshalling purposes.
     */
    public ServiceDeploymentExchangeId() {
    }

    /**
     * @param evt Cause discovery event.
     */
    public ServiceDeploymentExchangeId(DiscoveryEvent evt) {
        this.nodeId = evt.eventNode().id();
        this.topVer = evt.topologyVersion();
        this.evtType = evt.type();

        if (evt instanceof DiscoveryCustomEvent)
            this.reqId = ((DiscoveryCustomEvent)evt).customMessage().id();
        else
            this.reqId = null;
    }

    /**
     * @return Event node id.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Topology version.
     */
    public long topologyVersion() {
        return topVer;
    }

    /**
     * @return Event type.
     */
    public int eventType() {
        return evtType;
    }

    /**
     * @return Requests id.
     */
    public IgniteUuid requestId() {
        return reqId;
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
                if (!writer.writeUuid("nodeId", nodeId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeLong("topVer", topVer))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeInt("evtType", evtType))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeIgniteUuid("reqId", reqId))
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
                nodeId = reader.readUuid("nodeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                topVer = reader.readLong("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                evtType = reader.readInt("evtType");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                reqId = reader.readIgniteUuid("reqId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(ServiceDeploymentExchangeId.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 137;
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
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ServiceDeploymentExchangeId id = (ServiceDeploymentExchangeId)o;

        return topVer == id.topVer && evtType == id.evtType &&
            F.eq(nodeId, id.nodeId) && F.eq(reqId, id.reqId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(nodeId, topVer, evtType, reqId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServiceDeploymentExchangeId.class, this);
    }
}
