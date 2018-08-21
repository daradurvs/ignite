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
 * Services assignments request discovery message.
 */
public class ServicesAssignmentsRequestMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Unique custom message ID. */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** Assignments initiator id. */
    private final UUID snd;

    /** Exchange id. */
    private final ServicesDeploymentExchangeId exchId;

    /** Topology version. */
    private final long topVer;

    /** New services assignments. */
    @GridToStringInclude
    private Map<IgniteUuid, Map<UUID, Integer>> srvcsToReassign;

    /** Services assignments to apply. */
    @GridToStringInclude
    private Collection<GridServiceAssignments> srvcsToDeploy;

    /** Services names to undeploy. */
    @GridToStringInclude
    private Collection<IgniteUuid> srvcsToUndeploy;

    /**
     * @param snd Sender id.
     * @param exchId Exchange id.
     * @param topVer Topology version.
     */
    public ServicesAssignmentsRequestMessage(UUID snd, ServicesDeploymentExchangeId exchId, long topVer) {
        this.snd = snd;
        this.exchId = exchId;
        this.topVer = topVer;
        this.srvcsToReassign = Collections.emptyMap();
        this.srvcsToDeploy = Collections.emptySet();
        this.srvcsToUndeploy = Collections.emptySet();
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
     * Topology version.
     */
    public long topologyVersion() {
        return topVer;
    }

    /**
     * @return Services assignments.
     */
    public Map<IgniteUuid, Map<UUID, Integer>> assignments() {
        return srvcsToReassign;
    }

    /**
     * @param assigns Services assignments.
     */
    public void assignments(Map<IgniteUuid, Map<UUID, Integer>> assigns) {
        this.srvcsToReassign = assigns;
    }

    /**
     * @return Services assignments to apply.
     */
    public Collection<GridServiceAssignments> servicesToDeploy() {
        return srvcsToDeploy;
    }

    /**
     * @param servicesToDeploy Services assignments to apply.
     */
    public void servicesToDeploy(Collection<GridServiceAssignments> servicesToDeploy) {
        this.srvcsToDeploy = servicesToDeploy;
    }

    /**
     * @return Services names to undeploy.
     */
    public Collection<IgniteUuid> servicesToUndeploy() {
        return srvcsToUndeploy;
    }

    /**
     * @param undepSrvcsNames Services names to undeploy.
     */
    public void servicesToUndeploy(Collection<IgniteUuid> undepSrvcsNames) {
        this.srvcsToUndeploy = undepSrvcsNames;
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