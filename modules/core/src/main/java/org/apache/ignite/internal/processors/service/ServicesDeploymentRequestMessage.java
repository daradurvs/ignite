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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.services.ServiceConfiguration;
import org.jetbrains.annotations.Nullable;

/**
 * Services deployment request discovery message.
 */
public class ServicesDeploymentRequestMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Unique custom message ID. */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** Deployment initiator id. */
    private final UUID nodeId;

    /** Services configurations to deploy. */
    @GridToStringInclude
    private final List<ServiceConfiguration> srvcsToDeploy;

    /** Services names to undeploy. */
    @GridToStringInclude
    private final Set<String> srvcsToUndeploy;

    /**
     * @param nodeId Deployment initiator id.
     * @param srvcsToDeploy Services configurations to deploy.
     */
    public ServicesDeploymentRequestMessage(UUID nodeId, List<ServiceConfiguration> srvcsToDeploy) {
        this(nodeId, srvcsToDeploy, Collections.emptySet());
    }

    /**
     * @param nodeId Deployment initiator id.
     * @param srvcsToUndeploy Services names to undeploy.
     */
    public ServicesDeploymentRequestMessage(UUID nodeId, Set<String> srvcsToUndeploy) {
        this(nodeId, Collections.emptyList(), srvcsToUndeploy);
    }

    /**
     * @param nodeId Deployment initiator id.
     * @param srvcsToDeploy Services configurations to deploy.
     * @param srvcsToUndeploy Services names to undeploy.
     */
    private ServicesDeploymentRequestMessage(
        UUID nodeId,
        List<ServiceConfiguration> srvcsToDeploy,
        Set<String> srvcsToUndeploy
    ) {
        assert (srvcsToDeploy.isEmpty() && !srvcsToUndeploy.isEmpty()) ||
            (!srvcsToDeploy.isEmpty() && srvcsToUndeploy.isEmpty());

        this.nodeId = nodeId;
        this.srvcsToDeploy = srvcsToDeploy;
        this.srvcsToUndeploy = srvcsToUndeploy;
    }

    /**
     * @return Deployment initiator id.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Services configurations to deploy.
     */
    public List<ServiceConfiguration> servicesToDeploy() {
        return srvcsToDeploy;
    }

    /**
     * @return Services names to undeploy.
     */
    public Set<String> servicesToUndeploy() {
        return srvcsToUndeploy;
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
        return S.toString(ServicesDeploymentRequestMessage.class, this);
    }
}
