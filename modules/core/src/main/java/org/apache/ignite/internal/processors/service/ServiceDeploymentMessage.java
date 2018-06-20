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

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.services.ServiceConfiguration;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class ServiceDeploymentMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** Deployment initiator id. */
    private final UUID nodeId;

    /** */
    private final ServiceConfiguration cfg;

    /** */
    public Action act;

    /** Topology version. */
    @Nullable public Long topVer;

    /** Assignments. */
    @Nullable public Map<UUID, Integer> assigns;

    /**
     * @param nodeId Node id.
     * @param cfg Config.
     */
    public ServiceDeploymentMessage(UUID nodeId, ServiceConfiguration cfg) {
        this.nodeId = nodeId;
        this.cfg = cfg;

        this.act = Action.DEPLOY;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean stopProcess() {
        return act == Action.DEPLOY;
    }

    /** {@inheritDoc} */
    @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
        DiscoCache discoCache) {
        return null;
    }

    /**
     * @return Deployment initiator id.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Service configuration.
     */
    public ServiceConfiguration config() {
        return cfg;
    }

    /**
     * @return Service name;
     */
    public String name() {
        return cfg.getName();
    }

    /**
     * @param assigns Sets assignments.
     */
    public void assignments(Map<UUID, Integer> assigns, long topVer) {
        this.assigns = assigns;
        this.topVer = topVer;

        this.act = Action.ASSIGN;
    }

    /** */
    public enum Action {
        /** */
        DEPLOY,

        /** */
        ASSIGN,

        /** */
        DEPLOYED,

        /** */
        CANCEL
    }
}
