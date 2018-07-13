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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

public class ServicesAssignmentsExchangeManager {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final ServicesMapExchangeWorker exchWorker;

    /**
     * @param ctx Grid kernal context.
     */
    public ServicesAssignmentsExchangeManager(GridKernalContext ctx) {
        this.ctx = ctx;
        this.exchWorker = new ServicesMapExchangeWorker(
            ctx.igniteInstanceName(),
            "services-map-exchange",
            ctx.log(getClass())
        );

        new IgniteThread(exchWorker).start();
    }

    /**
     * @param topVer Topology version.
     * @return Added exchange future.
     */
    public synchronized ServicesAssignmentsExchangeFuture onEvent(ServicesAssignmentsExchangeFuture fut,
        AffinityTopologyVersion topVer) {
        Collection<ClusterNode> nodes = ctx.discovery().nodes(topVer.topologyVersion());

        Set<UUID> remaining = nodes.stream().map(ClusterNode::id).collect(Collectors.toSet());

        fut.remaining(remaining);

        exchWorker.q.offer(fut);

        return fut;
    }

    private final Object mux = new Object();

    List<ServicesSingleAssignmentsMessage> pending = new ArrayList<>();

    /**
     * @param snd
     * @param msg
     */
    public synchronized void onReceiveSingleMessage(final UUID snd, final ServicesSingleAssignmentsMessage msg) {
        ServicesAssignmentsExchangeFuture fut = exchWorker.fut;

        if (fut == null) {
            pending.add(msg);

            return;
        }

        if (fut.exchId.equals(msg.exchId))
            fut.onReceiveSingleMessage(snd, msg, msg.client);
        else
            pending.add(msg);
    }

    List<ServicesFullAssignmentsMessage> pendingFull = new ArrayList<>();

    public synchronized void onReceiveFullMessage(ServicesFullAssignmentsMessage msg) {
        ServicesAssignmentsExchangeFuture fut = exchWorker.fut;

        if (!fut.exchId.equals(msg.exchId))
            throw new IllegalStateException();

        fut.onDone();
    }

    /** */
    private class ServicesMapExchangeWorker extends GridWorker {
        /** */
        private final LinkedBlockingQueue<ServicesAssignmentsExchangeFuture> q = new LinkedBlockingQueue<>();

        volatile ServicesAssignmentsExchangeFuture fut = null;

        /** {@inheritDoc} */
        protected ServicesMapExchangeWorker(@Nullable String igniteInstanceName, String name,
            IgniteLogger log) {
            super(igniteInstanceName, name, log);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            while (!isCancelled()) {
                fut = q.poll();

                if (fut == null)
                    continue;

                fut.init();

                for (ServicesSingleAssignmentsMessage msg : pending) {
                    if (fut.exchId.equals(msg.exchId))
                        fut.onReceiveSingleMessage(msg.snd, msg, msg.client);
                }

                while (true) {
                    try {
                        for (ServicesSingleAssignmentsMessage msg : pending) {
                            if (fut.exchId.equals(msg.exchId))
                                fut.onReceiveSingleMessage(msg.snd, msg, msg.client);
                        }

                        fut.get(5_000);

                        break;
                    }
                    catch (IgniteCheckedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
