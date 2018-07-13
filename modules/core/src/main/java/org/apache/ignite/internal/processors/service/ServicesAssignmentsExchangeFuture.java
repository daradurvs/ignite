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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteUuid;

import static org.apache.ignite.internal.GridTopic.TOPIC_SERVICES;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SERVICE_POOL;

/**
 *
 */
public abstract class ServicesAssignmentsExchangeFuture extends GridFutureAdapter<Object> {
    /** */
    private final Map<String, Map<UUID, Integer>> fullAssignments = new ConcurrentHashMap<>();

    /** Remaining nodes. */
    private Set<UUID> remaining = new HashSet<>();

    Set<UUID> nodes = new HashSet<>();

    public abstract void init();

    IgniteUuid exchId;

    GridKernalContext ctx;

    /**
     * @param snd Sender.
     * @param msg Single node services assignments.
     */
    public synchronized void onReceiveSingleMessage(final UUID snd, final ServicesSingleAssignmentsMessage msg,
        boolean client) {
        if (remaining.remove(snd)) {
            if (!client) {
                for (Map.Entry<String, Integer> entry : msg.assigns().entrySet()) {
                    String name = entry.getKey();

                    Map<UUID, Integer> cur = fullAssignments.computeIfAbsent(name, k -> new HashMap<>());

                    cur.put(snd, entry.getValue());
                }
            }

            if (remaining.isEmpty()) {
                ServicesFullAssignmentsMessage fullMapMsg = createFullAssignmentsMessage();

                for (UUID node : nodes) {
                    try {
                        ctx.io().sendToGridTopic(node, TOPIC_SERVICES, fullMapMsg, SERVICE_POOL);
                    }
                    catch (IgniteCheckedException e) {
                        e.printStackTrace();
                    }
                }

                onDone();
            }
        }
    }

    public ServicesFullAssignmentsMessage createFullAssignmentsMessage() {
        // TODO: handle errors
        ServicesFullAssignmentsMessage msg = new ServicesFullAssignmentsMessage();

        msg.exchId = exchId;
        msg.snd = ctx.localNodeId();

        Map<String, ServiceAssignmentsMap> assigns = new HashMap<>();

        for (Map.Entry<String, Map<UUID, Integer>> entry : fullAssignments.entrySet())
            assigns.put(entry.getKey(), new ServiceAssignmentsMap(entry.getValue()));

        msg.assigns(assigns);

        return msg;
    }

    /**
     * @return Nodes ids to wait messages.
     */
    public Set<UUID> remaining() {
        return remaining;
    }

    /**
     * @param remaining Nodes ids to wait messages.
     */
    public void remaining(Set<UUID> remaining) {
        this.remaining = remaining;
    }

    /**
     * @return Services assignments.
     */
    public Map<String, Map<UUID, Integer>> assignments() {
        return fullAssignments;
    }
}
