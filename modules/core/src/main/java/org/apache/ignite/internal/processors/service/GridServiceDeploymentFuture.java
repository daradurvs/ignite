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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.services.ServiceConfiguration;

/**
 * Service deployment future.
 */
public class GridServiceDeploymentFuture extends GridFutureAdapter<Object> {
    /** */
    private final ServiceConfiguration cfg;

    /** Deployment initiators ids, to notify about deployment results. */
    private final Set<UUID> initiators;

    /** Contains errors during deployment. */
    private final Map<UUID, byte[]> errors;

    /** Nodes ids which were involved in deployment process, to synchronize deployment across the cluster. */
    private final Set<UUID> participants;

    /** */
    IgniteUuid exchId = null;

    /**
     * @param cfg Configuration.
     */
    public GridServiceDeploymentFuture(ServiceConfiguration cfg) {
        this.cfg = cfg;
        this.errors = new HashMap<>();
        this.initiators = new LinkedHashSet<>();
        this.participants = new HashSet<>();
    }

    /**
     * @param id Deployment initiator id.
     */
    public void registerInitiator(UUID id) {
        initiators.add(id);
    }

    /**
     * @return Set of deployment initiators ids.
     */
    public Set<UUID> initiators() {
        return Collections.unmodifiableSet(initiators);
    }

    /**
     * @return Nodes ids which were involved in deployment process.
     */
    public Set<UUID> participants() {
        return participants;
    }

    /**
     * @param participants Nodes ids which were involved in deployment process.
     */
    public void participants(Set<UUID> participants) {
        this.participants.addAll(participants);
    }

    /**
     * @return Errors occurred during deployment process.
     */
    public Map<UUID, byte[]> errors() {
        return errors;
    }

    /**
     * @param nodeId Node id where error has been occurred.
     * @param errBytes Serialized errors bytes.
     */
    public void error(UUID nodeId, byte[] errBytes) {
        errors.put(nodeId, errBytes);
    }

    /**
     * @return Service configuration.
     */
    public ServiceConfiguration configuration() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridServiceDeploymentFuture fut = (GridServiceDeploymentFuture)o;

        return cfg.equalsIgnoreNodeFilter(fut.cfg);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(cfg);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridServiceDeploymentFuture.class, this);
    }
}
