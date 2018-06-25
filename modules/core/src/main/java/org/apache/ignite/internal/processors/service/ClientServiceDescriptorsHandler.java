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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.GridTopic.TOPIC_SERVICES;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SERVICE_POOL;

/**
 *
 */
public class ClientServiceDescriptorsHandler implements GridMessageListener {
    /** Kernal context. */
    @GridToStringExclude
    private final GridKernalContext ctx;

    /** Grid logger. */
    @GridToStringExclude
    private final IgniteLogger log;

    /**
     * @param ctx Kernal context.
     */
    public ClientServiceDescriptorsHandler(GridKernalContext ctx) {
        this.ctx = ctx;
        this.log = ctx.log(getClass());
    }

    /** Service descriptor request futures. */
    private final Map<UUID, ServiceAssignmentsFuture> futs = new ConcurrentHashMap<>(1);

    /**
     * TODO: request only one assign.
     *
     * @param name Service name.
     * @return Service assignment.
     */
    public synchronized GridServiceAssignments serviceAssignment(String name) {
        Collection<GridServiceAssignments> assigns = serviceAssignments();

        for (GridServiceAssignments assign : assigns)
            if (assign.name().equals(name))
                return assign;

        return null;
    }

    /**
     * @return Collection of service assignments.
     */
    public synchronized Collection<GridServiceAssignments> serviceAssignments() {
        ServiceAssignmentsFuture fut = futs.get(ctx.localNodeId());

        try {
            if (fut == null) {
                fut = new ServiceAssignmentsFuture();

                futs.put(ctx.localNodeId(), fut);

                ServiceAssignmentsRequestMessage req = new ServiceAssignmentsRequestMessage();

                ClusterNode cdr = U.oldest(ctx.discovery().nodes(ctx.discovery().topologyVersion()), null);

                ctx.io().sendToGridTopic(cdr, TOPIC_SERVICES, req, SERVICE_POOL);
            }

            fut.get();

            return fut.assigns;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
        if (!(msg instanceof ServiceAssignmentsResponseMessage))
            return;

        synchronized (futs) {
            ServiceAssignmentsFuture fut = futs.get(ctx.localNodeId());

            if (fut != null) {
                Collection<byte[]> arrs = ((ServiceAssignmentsResponseMessage)msg).assignments();

                List<GridServiceAssignments> assigns = new ArrayList<>();

                for (byte[] arr : arrs) {
                    try {
                        GridServiceAssignments assign = U.unmarshal(ctx, arr, null);

                        assigns.add(assign);
                    }
                    catch (IgniteCheckedException e) {
                        log.error("Error during GridServiceAssignment unmarshalling.", e);
                    }
                }

                fut.assigns = assigns;

                fut.onDone();
            }
        }
    }

    /**
     * Service assignments request future.
     */
    private class ServiceAssignmentsFuture extends GridFutureAdapter<Object> {
        /** Services assignments. */
        private Collection<GridServiceAssignments> assigns;
    }
}
