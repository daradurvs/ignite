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
 * TODO: description
 */
public class DynamicServiceChangeRequest implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final byte DEPLOY_REQUEST = 0b0001;

    /** */
    private static final byte ASSIGNMENTS_REQUEST = 0b0010;

    /** */
    private static final byte CANCEL_REQUEST = 0b0100;

    /** Unique custom message ID. */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** Flags. */
    private byte flags;

    /** Deployment initiator id. */
    private final UUID nodeId;

    /** */
    private final ServiceConfiguration cfg;

    /** Topology version. */
    private long topVer;

    /** Assignments. */
    @Nullable private Map<UUID, Integer> assigns;

    /**
     * @param nodeId Node id.
     * @param cfg Config.
     */
    private DynamicServiceChangeRequest(UUID nodeId, ServiceConfiguration cfg) {
        this.nodeId = nodeId;
        this.cfg = cfg;
    }

    /**
     * @param nodeId Initiator node id.
     * @param cfg Service configuration.
     * @return Service deploy request.
     */
    public static DynamicServiceChangeRequest deployRequest(UUID nodeId, ServiceConfiguration cfg) {
        DynamicServiceChangeRequest req = new DynamicServiceChangeRequest(nodeId, cfg);

        req.markDeploy();

        return req;
    }

    /**
     * @param nodeId Initiator node id.
     * @param cfg Service configuration.
     * @param assigns Service assignments.
     * @param topVer Topology version.
     * @return Service assignments request.
     */
    public static DynamicServiceChangeRequest assignmentsRequest(
        UUID nodeId,
        ServiceConfiguration cfg,
        Map<UUID, Integer> assigns,
        long topVer
    ) {
        DynamicServiceChangeRequest req = new DynamicServiceChangeRequest(nodeId, cfg);

        req.assignments(assigns, topVer);
        req.markAssignments();

        return req;
    }

    /**
     * @param nodeId Deployment initiator node id.
     * @param cfg Service configuration.
     * @return Service cancel request.
     */
    public static DynamicServiceChangeRequest cancelRequest(UUID nodeId, ServiceConfiguration cfg) {
        DynamicServiceChangeRequest req = new DynamicServiceChangeRequest(nodeId, cfg);

        req.markCancel();

        return req;
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
    public ServiceConfiguration configuration() {
        return cfg;
    }

    /**
     * @return Service name.
     */
    public String name() {
        return cfg.getName();
    }

    /**
     * @return Topology version.
     */
    public long topologyVersion() {
        return topVer;
    }

    /**
     * @return Service assignments.
     */
    @Nullable public Map<UUID, Integer> assignments() {
        return assigns;
    }

    /**
     * @param assigns Sets assignments.
     */
    void assignments(Map<UUID, Integer> assigns, long topVer) {
        this.assigns = assigns == null ? Collections.EMPTY_MAP : assigns;
        this.topVer = topVer;
    }

    /**
     * Mark that request type as deploy.
     */
    void markDeploy() {
        flags |= DEPLOY_REQUEST;
    }

    /**
     * @return
     */
    boolean isDeploy() {
        return (flags & DEPLOY_REQUEST) != 0;
    }

    /**
     * Mark that request type as assignment.
     */
    void markAssignments() {
        flags |= ASSIGNMENTS_REQUEST;
    }

    /**
     * @return
     */
    boolean isAssignments() {
        return (flags & ASSIGNMENTS_REQUEST) != 0;
    }

    /**
     * Mark that request type as cancel.
     */
    void markCancel() {
        flags |= CANCEL_REQUEST;
    }

    /**
     * @return
     */
    boolean isCancel() {
        return (flags & CANCEL_REQUEST) != 0;
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
        return !isDeploy();
    }

    /** {@inheritDoc} */
    @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
        DiscoCache discoCache) {
        // No-op.
        return null;
    }
}
