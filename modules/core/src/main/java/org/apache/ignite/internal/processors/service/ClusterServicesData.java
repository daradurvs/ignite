/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.service;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedHashMap;

/**
 * Cluster services unhandled joining nodes data.
 */
public class ClusterServicesData {
    /** Local node id. */
    private final UUID locNodeId;

    /** Local node's joining data. */
    private final AtomicReference<ServicesJoiningNodeDiscoveryData> locJoiningData = new AtomicReference<>();

    /** Unhandled cluster joining nodes data. */
    private final ConcurrentMap<UUID, ServicesJoiningNodeDiscoveryData> joiningNodesData = new ConcurrentLinkedHashMap<>();

    /**
     * @param locNodeId Local node id.
     */
    public ClusterServicesData(UUID locNodeId) {
        this.locNodeId = locNodeId;
    }

    /**
     * @param data Local node's joining data.
     */
    public void onStart(ServicesJoiningNodeDiscoveryData data) {
        locJoiningData.set(data);
    }

    /**
     * @param nodeId Joining node id.
     * @param data Data.
     */
    public void onJoiningNodeDataReceived(UUID nodeId, ServicesJoiningNodeDiscoveryData data) {
        joiningNodesData.put(nodeId, data);
    }

    /**
     * Returns unhandled data fow given node id.
     *
     * @param nodeId Node id.
     * @return Data.
     */
    @Nullable public ServicesJoiningNodeDiscoveryData get(UUID nodeId) {
        if (locNodeId.equals(nodeId))
            return locJoiningData.get();

        return joiningNodesData.get(nodeId);
    }

    /**
     * Returns and remove unhandled data fow given node id.
     *
     * @param nodeId Node id.
     * @return Data.
     */
    @Nullable public ServicesJoiningNodeDiscoveryData remove(UUID nodeId) {
        if (locNodeId.equals(nodeId))
            return locJoiningData.getAndSet(null);

        return joiningNodesData.get(nodeId);
    }

    /**
     * @return New map of joining nodes data.
     */
    public HashMap<UUID, ServicesJoiningNodeDiscoveryData> data() {
        HashMap<UUID, ServicesJoiningNodeDiscoveryData> data = new LinkedHashMap<>(joiningNodesData);

        ServicesJoiningNodeDiscoveryData locData = locJoiningData.get();

        if (locData != null)
            data.put(locNodeId, locData);

        return data;
    }
}
