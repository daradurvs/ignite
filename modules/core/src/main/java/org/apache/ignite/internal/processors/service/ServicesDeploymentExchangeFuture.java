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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.services.ServiceConfiguration;

/**
 *
 */
public class ServicesDeploymentExchangeFuture extends GridFutureAdapter<Object> {
    /** */
    private final Map<UUID, ServicesSingleAssignmentsMessage> singleAssignsMessages = new ConcurrentHashMap<>();

    private final ServicesAssignmentsFunction assignsFunc;

    private final GridKernalContext ctx;

    private final IgniteLogger log;

    private final DiscoveryEvent evt;

    private final IgniteUuid exchId;

    /** Remaining nodes. */
    private final Set<UUID> remaining;

    Map<String, GridServiceAssignments> svcAssigns;

    public ServicesDeploymentExchangeFuture(ServicesAssignmentsFunction assignsFunc, GridKernalContext ctx,
        DiscoveryEvent evt) {
        assert evt instanceof DiscoveryCustomEvent;

        this.assignsFunc = assignsFunc;
        this.ctx = ctx;
        this.evt = evt;
        this.exchId = ((DiscoveryCustomEvent)evt).customMessage().id();

        this.log = ctx.log(getClass());
        this.remaining = ctx.discovery().nodes(evt.topologyVersion()).stream().map(ClusterNode::id).distinct().collect(Collectors.toSet());
    }

    public void init() {
        if (log.isDebugEnabled())
            log.debug("Started init method: [exchId=" + exchangeId() + "; locId=" + ctx.localNodeId() + ']');

        DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)evt).customMessage();

        try {
            if (msg instanceof ServicesCancellationRequestMessage) {
                ServicesCancellationRequestMessage msg0 = (ServicesCancellationRequestMessage)msg;
                Collection<String> names = msg0.names();

                ServicesFullAssignmentsMessage fullMsg = new ServicesFullAssignmentsMessage();

                fullMsg.exchId = exchId;
                fullMsg.snd = ctx.localNodeId();

                Map<String, ServiceAssignmentsMap> assigns = new HashMap<>();

                svcAssigns.forEach((name, svcMap) -> {
                    if (!names.contains(name))
                        assigns.put(name, new ServiceAssignmentsMap(svcMap.assigns()));
                });

                fullMsg.assigns(assigns);

                try {
                    ctx.discovery().sendCustomEvent(fullMsg);
                }
                catch (IgniteCheckedException e) {
                    e.printStackTrace();
                }
            }
            else if (msg instanceof ServicesDeploymentRequestMessage) {
                onDeploymentRequest(evt.eventNode().id(), (ServicesDeploymentRequestMessage)msg, ((DiscoveryCustomEvent)evt).affinityTopologyVersion());
                //                Executors.newSingleThreadExecutor().execute(() -> {
//                        try {
//                            ctx.service().onDeploymentRequest(evt.eventNode().id(), (ServicesDeploymentRequestMessage)msg, ((DiscoveryCustomEvent)evt).affinityTopologyVersion());
//                        }
//                        catch (IgniteCheckedException e) {
//                            e.printStackTrace();
//                        }
//                    }
//                );
            }
            else
                throw new IllegalStateException("Unexpected message type: " + msg);
        }
        catch (Exception e) {
            log.error("Exception occurred inside init method: " + e);
        }

        if (log.isDebugEnabled())
            log.debug("Finished init method: [exchId=" + exchangeId() + "; locId=" + ctx.localNodeId() + ']');
    }

    /**
     * @param snd Sender.
     * @param msg Single node services assignments.
     */
    public synchronized void onReceiveSingleMessage(final UUID snd, final ServicesSingleAssignmentsMessage msg,
        boolean client) {
        assert exchId.equals(msg.exchId) : "Wrong message exchId!";

        if (remaining.remove(snd)) {
            if (!client)
                singleAssignsMessages.put(snd, msg);

            if (remaining.isEmpty()) {
                ServicesFullAssignmentsMessage fullMapMsg = createFullAssignmentsMessage();

                if (((DiscoveryCustomEvent)evt).customMessage() instanceof ServicesDeploymentRequestMessage)
                    if (fullMapMsg.assigns().isEmpty())
                        log.info("****");

                try {
                    ctx.discovery().sendCustomEvent(fullMapMsg);
                }
                catch (IgniteCheckedException e) {
                    e.printStackTrace();
                }

//                for (UUID node : nodes) {
//                    try {
//                        ctx.io().sendToGridTopic(node, TOPIC_SERVICES, fullMapMsg, SERVICE_POOL);
//                    }
//                    catch (IgniteCheckedException e) {
//                        log.error("Failed to send services full assignments to node: " + node, e);
//                    }
//                }
            }
        }
        else
            System.out.println("Unexpected message: " + msg);
    }

    void onDeploymentRequest(UUID snd, ServicesDeploymentRequestMessage req,
        AffinityTopologyVersion topVer) throws IgniteCheckedException {
        Collection<ServiceConfiguration> cfgs = req.configurations();

        Collection<GridServiceAssignments> assigns = new ArrayList<>(cfgs.size());

        for (ServiceConfiguration cfg : cfgs) {
            GridServiceAssignments svcAssigns = assignsFunc.reassign(cfg, snd, topVer);

            if (log.isDebugEnabled())
                log.debug("Calculated assignment: " + svcAssigns.assigns());

            assigns.add(svcAssigns);
        }

        ServicesAssignmentsRequestMessage assignsMsg = new ServicesAssignmentsRequestMessage(snd, assigns);

        assignsMsg.exchId = exchId;

        ctx.discovery().sendCustomEvent(assignsMsg);
    }

    public synchronized ServicesFullAssignmentsMessage createFullAssignmentsMessage() {
        // TODO: handle errors
        ServicesFullAssignmentsMessage fullMsg = new ServicesFullAssignmentsMessage();

        fullMsg.exchId = exchId;
        fullMsg.snd = ctx.localNodeId();

        Map<String, ServiceAssignmentsMap> assigns = new HashMap<>();

        Map<String, Map<UUID, Integer>> fullAssignments = new ConcurrentHashMap<>();

        singleAssignsMessages.forEach((uuid, singleMsg) -> {
            singleMsg.assigns().forEach((name, num) -> {
                if (num != 0) {
                    Map<UUID, Integer> cur = fullAssignments.computeIfAbsent(name, m -> new HashMap<>());

                    cur.put(uuid, num);
                }
            });
        });

        for (Map.Entry<String, Map<UUID, Integer>> entry : fullAssignments.entrySet())
            assigns.put(entry.getKey(), new ServiceAssignmentsMap(entry.getValue()));

        fullMsg.assigns(assigns);

        return fullMsg;
    }

    public IgniteUuid exchangeId() {
        return exchId;
    }

    /**
     * @return Nodes ids to wait messages.
     */
    public Set<UUID> remaining() {
        return Collections.unmodifiableSet(remaining);
    }
}
