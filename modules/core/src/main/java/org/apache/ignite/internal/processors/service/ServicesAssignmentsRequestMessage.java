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

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Services assignments request discovery message.
 */
public class ServicesAssignmentsRequestMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Unique custom message ID. */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** Assignments initiator id. */
    private final UUID nodeId;

    /** Exchange id. */
    private final ServiceDeploymentExchangeId exchId;

    /** Services assignments. */
    @GridToStringInclude
    private final Collection<GridServiceAssignments> assigns;

    /**
     * @param nodeId Assignments initiator id.
     * @param exchId Exchange id.
     * @param assigns Services assignments.
     */
    public ServicesAssignmentsRequestMessage(UUID nodeId, ServiceDeploymentExchangeId exchId,
        Collection<GridServiceAssignments> assigns) {
        this.nodeId = nodeId;
        this.exchId = exchId;
        this.assigns = assigns;
    }

    /**
     * @return Assignments initiator id.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Exchange id.
     */
    public ServiceDeploymentExchangeId exchangeId() {
        return exchId;
    }

    /**
     * @return Services assignments.
     */
    public Collection<GridServiceAssignments> assignments() {
        return assigns;
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServicesAssignmentsRequestMessage.class, this);
    }
}