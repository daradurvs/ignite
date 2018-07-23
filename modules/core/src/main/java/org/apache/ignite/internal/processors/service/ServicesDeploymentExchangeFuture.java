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
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.services.ServiceConfiguration;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 * Services deployment exchange future.
 */
public class ServicesDeploymentExchangeFuture extends GridFutureAdapter<Object> {
    /** Single service messages to process. */
    private final Map<UUID, ServicesSingleAssignmentsMessage> singleAssignsMsgs = new ConcurrentHashMap<>();

    /** Errors occurred during assignments calculation. */
    private final Map<String, Throwable> reassignsErrors = new HashMap<>();

    /** Mutex. */
    private final Object mux = new Object();

    /** Services assignments. */
    private Map<String, GridServiceAssignments> srvcsAssigns;

    /** Services assignments function. */
    private final ServicesAssignmentsFunction assignsFunc;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Discovery event. */
    private final DiscoveryEvent evt;

    /** Topology version. */
    private final AffinityTopologyVersion evtTopVer;

    /** Exchange id. */
    private final IgniteUuid exchId;

    /** Remaining nodes to received single node assignments message. */
    private final Set<UUID> remaining;

    /**
     * @param srvcsAssigns Services assignments.
     * @param assignsFunc Services assignments function.
     * @param ctx Kernal context.
     * @param evt Discovery event.
     * @param evtTopVer Topology version.
     */
    public ServicesDeploymentExchangeFuture(Map<String, GridServiceAssignments> srvcsAssigns,
        ServicesAssignmentsFunction assignsFunc, GridKernalContext ctx, DiscoveryEvent evt,
        AffinityTopologyVersion evtTopVer) {
        this.srvcsAssigns = srvcsAssigns;
        this.assignsFunc = assignsFunc;
        this.ctx = ctx;
        this.evt = evt;
        this.evtTopVer = evtTopVer;
        this.exchId = evt.id();

        this.log = ctx.log(getClass());
        this.remaining = ctx.discovery().nodes(evt.topologyVersion()).stream().map(ClusterNode::id).distinct().collect(Collectors.toSet());
    }

    /**
     * Services assignments exchange initialization method.
     *
     * @throws Exception In case of an error.
     */
    public void init() throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("Started services exchange future init: [exchId=" + exchangeId() +
                ", locId=" + ctx.localNodeId() +
                ", evt=" + evt + ']');
        }

        if (evt instanceof DiscoveryCustomEvent) {
            DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)evt).customMessage();

            if (msg instanceof ServicesCancellationRequestMessage)
                onCancellationRequest((ServicesCancellationRequestMessage)msg, evtTopVer.topologyVersion());
            else if (msg instanceof ServicesDeploymentRequestMessage)
                onDeploymentRequest(evt.eventNode().id(), (ServicesDeploymentRequestMessage)msg, evtTopVer);
            else
                onDone(new IgniteIllegalStateException("Unexpected discovery custom message, msg=" + msg));
        }
        else {
            if (!srvcsAssigns.isEmpty()) {
                switch (evt.type()) {
                    case EVT_NODE_JOINED:
                    case EVT_NODE_LEFT:
                    case EVT_NODE_FAILED:

                        onChangedTopology(evtTopVer);

                        break;

                    default:
                        onDone(new IgniteIllegalStateException("Unexpected discovery event, evt=" + evt));

                        break;
                }
            }
            else
                onDone();
        }

        if (log.isDebugEnabled())
            log.debug("Finished services exchange future init: [exchId=" + exchangeId() + ", locId=" + ctx.localNodeId() + ']');
    }

    /**
     * @param snd Sender id.
     * @param req Services deployment request.
     * @param topVer Topology version.
     */
    private void onDeploymentRequest(UUID snd, ServicesDeploymentRequestMessage req, AffinityTopologyVersion topVer) {
        Collection<ServiceConfiguration> cfgs = req.configurations();

        Collection<GridServiceAssignments> assigns = new ArrayList<>(cfgs.size());

        for (ServiceConfiguration cfg : cfgs) {
            GridServiceAssignments svcAssigns;

            try {
                svcAssigns = assignsFunc.reassign(cfg, snd, topVer);
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to calculate assignment for service, cfg=" + cfg, e);

                reassignsErrors.put(cfg.getName(), e);

                continue;
            }

            if (log.isDebugEnabled())
                log.debug("Calculated service assignments: " + svcAssigns);

            assigns.add(svcAssigns);
        }

        ServicesAssignmentsRequestMessage msg = new ServicesAssignmentsRequestMessage(snd, exchId, assigns);

        try {
            ctx.discovery().sendCustomEvent(msg);
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to send services assignments request message across the ring, msg=" + msg, e);
        }
    }

    /**
     * @param req Services cancellation request.
     * @param topVer Topology version.
     */
    private void onCancellationRequest(ServicesCancellationRequestMessage req, long topVer) {
        Collection<String> names = req.names();

        Map<String, ServiceAssignmentsMap> assigns = new HashMap<>();

        srvcsAssigns.forEach((name, svcMap) -> {
            if (!names.contains(name))
                assigns.put(name, new ServiceAssignmentsMap(name, svcMap.assigns(), topVer));
        });

        ServicesFullAssignmentsMessage msg = new ServicesFullAssignmentsMessage(ctx.localNodeId(), exchId, assigns);

        try {
            ctx.discovery().sendCustomEvent(msg);
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to send services full assignments message across the ring, msg=" + msg, e);
        }
    }

    /**
     *
     */
    private void checkAndProcess() {
        if (remaining.isEmpty()) {
            ServicesFullAssignmentsMessage msg = createFullAssignmentsMessage();

            try {
                ctx.discovery().sendCustomEvent(msg);
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to send full services assignment across the ring.", e);
            }
        }
    }

    /**
     * @return Services full assignments message.
     */
    private ServicesFullAssignmentsMessage createFullAssignmentsMessage() {
        synchronized (mux) {
            final Map<String, ServiceAssignmentsMap> assigns = new HashMap<>();

            final Map<String, Map<UUID, Integer>> fullAssigns = new HashMap<>();

            final Map<String, Collection<byte[]>> fullErrors = new HashMap<>();

            try {
                singleAssignsMsgs.forEach((uuid, singleMsg) -> {
                    singleMsg.assigns().forEach((name, num) -> {
                        if (num != 0) {
                            Map<UUID, Integer> cur = fullAssigns.computeIfAbsent(name, m -> new HashMap<>());

                            cur.put(uuid, num);
                        }
                    });

                    singleMsg.errors().forEach((name, err) -> {
                        Collection<byte[]> srvcErrors = fullErrors.computeIfAbsent(name, e -> new ArrayList<>());

                        srvcErrors.add(err);
                    });
                });

                fullAssigns.forEach((name, svcAssigns) -> {
                    assigns.put(name, new ServiceAssignmentsMap(name, svcAssigns, evt.topologyVersion()));
                });

                reassignsErrors.forEach((name, err) -> {
                    byte[] arr = null;

                    try {
                        arr = U.marshal(ctx, err);
                    }
                    catch (IgniteCheckedException e) {
                        log.error("Failed to marshal reassignments error.", e);
                    }

                    if (arr != null)
                        fullErrors.putIfAbsent(name, Collections.singleton(arr));
                });
            }
            catch (Throwable t) {
                log.error("Failed to build services full assignments map.", t);
            }

            ServicesFullAssignmentsMessage msg = new ServicesFullAssignmentsMessage(ctx.localNodeId(), exchId, assigns);

            if (!fullErrors.isEmpty())
                msg.errors(fullErrors);

            return msg;
        }
    }

    /**
     * @param msg Single node services assignments.
     */
    public void onReceiveSingleMessage(final ServicesSingleAssignmentsMessage msg) {
        synchronized (mux) {
            assert exchId.equals(msg.exchangeId()) : "Wrong messages exchange id, msg=" + msg;

            if (remaining.remove(msg.senderId())) {
                if (!msg.client())
                    singleAssignsMsgs.put(msg.senderId(), msg);

                checkAndProcess();
            }
            else
                System.out.println("Unexpected message: " + msg);
        }
    }

    /**
     * @param topVer Topology version.
     */
    private void onChangedTopology(AffinityTopologyVersion topVer) {
        ServicesFullAssignmentsMessage msg = reassignAll(topVer);

        try {
            ctx.discovery().sendCustomEvent(msg);
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to send full services assignment across the ring.", e);
        }
    }

    /**
     * @param topVer Topology version.
     * @return Services full assignments message.
     */
    private ServicesFullAssignmentsMessage reassignAll(AffinityTopologyVersion topVer) {
        final Map<String, ServiceAssignmentsMap> assigns = new HashMap<>();

        srvcsAssigns.forEach((name, old) -> {
            GridServiceAssignments srvcAssigns = null;

            try {
                srvcAssigns = assignsFunc.reassign(old.configuration(), old.nodeId(), topVer);
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to recalculate assignments for service, previously calculated assignments will be used, cfg=" + old.configuration(), e);

                reassignsErrors.put(old.name(), e);

                srvcAssigns = old;
            }

            if (srvcAssigns != null) {
                if (log.isDebugEnabled())
                    log.debug("Calculated service assignments: " + srvcAssigns);

                assigns.put(name, new ServiceAssignmentsMap(name, srvcAssigns.assigns(), evt.topologyVersion()));
            }
        });

        Map<String, Collection<byte[]>> errors = new HashMap<>();

        reassignsErrors.forEach((name, err) -> {
            byte[] arr = null;

            try {
                arr = U.marshal(ctx, err);
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to marshal reassignments error.", e);
            }

            if (arr != null)
                errors.put(name, Collections.singleton(arr));
        });

        ServicesFullAssignmentsMessage msg = new ServicesFullAssignmentsMessage(ctx.localNodeId(), exchId, assigns);

        if (!errors.isEmpty())
            msg.errors(errors);

        return msg;
    }

    /**
     * @param nodeId Node id.
     */
    public void onNodeLeft(UUID nodeId) {
        synchronized (mux) {
            remaining.remove(nodeId);

            checkAndProcess();
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
