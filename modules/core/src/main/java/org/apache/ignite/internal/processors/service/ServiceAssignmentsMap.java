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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import static org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType.INT;
import static org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType.UUID;

/**
 * Single service assignments.
 */
public class ServiceAssignmentsMap implements Message {
    /** Service assignments. */
    private Map<UUID, Integer> assigns;

    /**
     * Empty constructor for marshalling purposes.
     */
    public ServiceAssignmentsMap() {
    }

    /**
     * @param assigns Service assignments.
     */
    public ServiceAssignmentsMap(Map<UUID, Integer> assigns) {
        this.assigns = assigns;
    }

    /**
     * @return Service assignments.
     */
    public Map<UUID, Integer> assigns() {
        return assigns;
    }

    /**
     * @param assigns Service assignments.
     */
    public void assigns(Map<UUID, Integer> assigns) {
        this.assigns = assigns;
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
                if (!writer.writeMap("assigns", assigns, UUID, INT))
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
                assigns = reader.readMap("assigns", UUID, INT, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(ServiceAssignmentsMap.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 136;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServiceAssignmentsMap.class, this);
    }
}
