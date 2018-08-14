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
import java.util.Collections;
import java.util.Map;
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
 * Services full map message.
 */
public class ServicesFullMapMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Unique custom message ID. */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** Sender node id. */
    private final UUID snd;

    /** Exchange id. */
    private final ServicesDeploymentExchangeId exchId;

    /** Cluster services assignments. */
    @GridToStringInclude
    private final Map<IgniteUuid, ServiceAssignmentsMap> assigns;

    /** Services deployments errors. */
    @GridToStringInclude
    private Map<IgniteUuid, Collection<byte[]>> errors;

    /**
     * @param snd Sender id.
     * @param exchId Exchange id.
     * @param assigns Services assignments.
     */
    public ServicesFullMapMessage(UUID snd, ServicesDeploymentExchangeId exchId,
        Map<IgniteUuid, ServiceAssignmentsMap> assigns) {
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
     * @return Exchange id.
     */
    public ServicesDeploymentExchangeId exchangeId() {
        return exchId;
    }

    /**
     * @return Services deployments errors.
     */
    public Map<IgniteUuid, Collection<byte[]>> errors() {
        return errors;
    }

    /**
     * @param errors Services deployments errors.
     */
    public void errors(Map<IgniteUuid, Collection<byte[]>> errors) {
        this.errors = errors;
    }

    /**
     * @return Local services assignments.
     */
    public Map<IgniteUuid, ServiceAssignmentsMap> assigns() {
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
        return S.toString(ServicesFullMapMessage.class, this);
    }
}
