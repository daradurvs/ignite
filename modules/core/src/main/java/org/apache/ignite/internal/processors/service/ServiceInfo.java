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
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDescriptor;
import org.jetbrains.annotations.Nullable;

/**
 * Service's information container.
 */
public class ServiceInfo implements ServiceDescriptor {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node ID. */
    private final UUID nodeId;

    /** Service configuration. */
    private final ServiceConfiguration cfg;

    /** Topology snapshot. */
    @GridToStringInclude
    private Map<UUID, Integer> top = Collections.emptyMap();

    /**
     * @param nodeId Initiating node id.
     * @param cfg Service configuration.
     */
    public ServiceInfo(UUID nodeId, ServiceConfiguration cfg) {
        this.nodeId = nodeId;
        this.cfg = cfg;
    }

    /**
     * Sets service's new topology snapshot.
     *
     * @param top Topology snapshot.
     */
    public void topologySnapshot(Map<UUID, Integer> top) {
        this.top = top;
    }

    /**
     * Returns service's configuration.
     *
     * @return Service configuration.
     */
    public ServiceConfiguration configuration() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return cfg.getName();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Class<? extends Service> serviceClass() {
        if (cfg instanceof LazyServiceConfiguration) {
            String clsName = ((LazyServiceConfiguration)cfg).serviceClassName();

            try {
                return (Class<? extends Service>)Class.forName(clsName);
            }
            catch (ClassNotFoundException e) {
                throw new IgniteException("Failed to find service class: " + clsName, e);
            }
        }
        else
            return cfg.getService().getClass();
    }

    /** {@inheritDoc} */
    @Override public int totalCount() {
        return cfg.getTotalCount();
    }

    /** {@inheritDoc} */
    @Override public int maxPerNodeCount() {
        return cfg.getMaxPerNodeCount();
    }

    /** {@inheritDoc} */
    @Nullable @Override public String cacheName() {
        return cfg.getCacheName();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public <K> K affinityKey() {
        return (K)cfg.getAffinityKey();
    }

    /** {@inheritDoc} */
    @Override public UUID originNodeId() {
        return nodeId;
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, Integer> topologySnapshot() {
        return top;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServiceInfo.class, this);
    }
}
