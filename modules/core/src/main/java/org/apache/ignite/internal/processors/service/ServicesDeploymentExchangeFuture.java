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
 * Services deployment exchange future.
 */
public class ServicesDeploymentExchangeFuture extends GridFutureAdapter<Object> {
    /** */
    private final Map<UUID, ServicesSingleAssignmentsMessage> singleAssignsMessages = new ConcurrentHashMap<>();

    /** Mutex. */
    private final Object mux = new Object();

    /** Services assignments. */
    private Map<String, GridServiceAssignments> svcsAssigns;

    /** Services assignments function. */
    private final ServicesAssignmentsFunction assignsFunc;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Discovery event. */
    private final DiscoveryEvent evt;

    /** Exchange id. */
    private final IgniteUuid exchId;

    /** Remaining nodes to received single node assignments message. */
    private final Set<UUID> remaining;

    /**
     * @param svcAssigns Services assignments.
     * @param assignsFunc Services assignments function.
     * @param ctx Kernal context.
     * @param evt Discovery event.
     */
    public ServicesDeploymentExchangeFuture(Map<String, GridServiceAssignments> svcAssigns,
        ServicesAssignmentsFunction assignsFunc, GridKernalContext ctx,
        DiscoveryEvent evt) {
        assert evt instanceof DiscoveryCustomEvent;

        this.svcsAssigns = svcAssigns;
        this.assignsFunc = assignsFunc;
        this.ctx = ctx;
        this.evt = evt;
        this.exchId = ((DiscoveryCustomEvent)evt).customMessage().id();

        this.log = ctx.log(getClass());
        this.remaining = ctx.discovery().nodes(evt.topologyVersion()).stream().map(ClusterNode::id).distinct().collect(Collectors.toSet());
    }

    /**
     * @throws IgniteCheckedException in case of an error.
     */
    public void init() throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Started services exchange init: [exchId=" + exchangeId() + "; locId=" + ctx.localNodeId() + ']');

        DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)evt).customMessage();

        try {
            if (msg instanceof ServicesCancellationRequestMessage)
                onCancellationRequest((ServicesCancellationRequestMessage)msg);
            else if (msg instanceof ServicesDeploymentRequestMessage)
                onDeploymentRequest(evt.eventNode().id(), (ServicesDeploymentRequestMessage)msg, ((DiscoveryCustomEvent)evt).affinityTopologyVersion());
            else
                throw new IllegalStateException("Unexpected message type: " + msg);
        }
        catch (Exception e) {
            log.error("Exception occurred inside services exchange init method: " + e);

            throw e;
        }

        if (log.isDebugEnabled())
            log.debug("Finished services excange init method: [exchId=" + exchangeId() + "; locId=" + ctx.localNodeId() + ']');
    }

    /**
     * @param snd Sender id.
     * @param req Services deployment request.
     * @param topVer Topology version.
     * @throws IgniteCheckedException In case of an error.
     */
    private void onDeploymentRequest(UUID snd, ServicesDeploymentRequestMessage req,
        AffinityTopologyVersion topVer) throws IgniteCheckedException {
        Collection<ServiceConfiguration> cfgs = req.configurations();

        Collection<GridServiceAssignments> assigns = new ArrayList<>(cfgs.size());

        for (ServiceConfiguration cfg : cfgs) {
            GridServiceAssignments svcAssigns = assignsFunc.reassign(cfg, snd, topVer);

            if (log.isDebugEnabled())
                log.debug("Calculated service assignments: " + svcAssigns);

            assigns.add(svcAssigns);
        }

        ServicesAssignmentsRequestMessage msg = new ServicesAssignmentsRequestMessage(snd, assigns);

        msg.exchId = exchId;

        ctx.discovery().sendCustomEvent(msg);
    }

    /**
     * @param req Services cancellation request.
     */
    private void onCancellationRequest(ServicesCancellationRequestMessage req) {
        Collection<String> names = req.names();

        Map<String, ServiceAssignmentsMap> assigns = new HashMap<>();

        svcsAssigns.forEach((name, svcMap) -> {
            if (!names.contains(name))
                assigns.put(name, new ServiceAssignmentsMap(svcMap.assigns()));
        });

        ServicesFullAssignmentsMessage msg = new ServicesFullAssignmentsMessage(ctx.localNodeId(), exchId, assigns);

        try {
            ctx.discovery().sendCustomEvent(msg);
        }
        catch (IgniteCheckedException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     */
    private void checkRemaining() {
        if (remaining.isEmpty()) {
            ServicesFullAssignmentsMessage fullMapMsg = createFullAssignmentsMessage();

            try {
                ctx.discovery().sendCustomEvent(fullMapMsg);
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to send full services assignment across the ring.", e);
            }
        }
    }

    /**
     * @return Services full assignment message.
     */
    private ServicesFullAssignmentsMessage createFullAssignmentsMessage() {
        synchronized (mux) {
            Map<String, ServiceAssignmentsMap> assigns = new HashMap<>();

            Map<String, Map<UUID, Integer>> fullAssigns = new HashMap<>();

            singleAssignsMessages.forEach((uuid, singleMsg) -> {
                singleMsg.assigns().forEach((name, num) -> {
                    if (num != 0) {
                        Map<UUID, Integer> cur = fullAssigns.computeIfAbsent(name, m -> new HashMap<>());

                        cur.put(uuid, num);
                    }
                });
            });

            for (Map.Entry<String, Map<UUID, Integer>> entry : fullAssigns.entrySet())
                assigns.put(entry.getKey(), new ServiceAssignmentsMap(entry.getValue()));

            // TODO: handle errors

            return new ServicesFullAssignmentsMessage(ctx.localNodeId(), exchId, assigns);
        }
    }

    /**
     * @param msg Single node services assignments.
     */
    public void onReceiveSingleMessage(final ServicesSingleAssignmentsMessage msg) {
        synchronized (mux) {
            assert exchId.equals(msg.exchangeId()) : "Wrong message exchId!";

            if (remaining.remove(msg.senderId())) {
                if (!msg.client())
                    singleAssignsMessages.put(msg.senderId(), msg);

                checkRemaining();
            }
            else
                System.out.println("Unexpected message: " + msg);
        }
    }

    /**
     * @param nodeId Node id.
     */
    public void onNodeLeft(UUID nodeId) {
        synchronized (mux) {
            remaining.remove(nodeId);

            checkRemaining();
        }
    }

    /**
     * @return Exchange id.
     */
    public IgniteUuid exchangeId() {
        return exchId;
    }

    /**
     * @return Nodes ids to wait single node assignments messages.
     */
    public Set<UUID> remaining() {
        return Collections.unmodifiableSet(remaining);
    }
}
