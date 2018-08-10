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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeRequest;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.services.ServiceConfiguration;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.services.ServiceDeploymentFailuresPolicy.IGNORE;

/**
 * Services deployment exchange future.
 */
public class ServicesDeploymentExchangeFutureTask extends GridFutureAdapter<Object> implements ServicesDeploymentExchangeTask {
    /** Single service messages to process. */
    @GridToStringInclude
    private final Map<UUID, ServicesSingleMapMessage> singleAssignsMsgs = new ConcurrentHashMap<>();

    /** Expected services assignments. */
    private final Map<String, Map<UUID, Integer>> expAssigns = new HashMap<>();

    /** Deployment errors. */
    @GridToStringInclude
    private final Map<String, Throwable> depErrors = new HashMap<>();

    /** Mutex. */
    private final Object mux = new Object();

    /** Services assignments. */
    private Map<String, GridServiceAssignments> srvcsAssigns;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Discovery event. */
    @GridToStringInclude
    private final DiscoveryEvent evt;

    /** Topology version. */
    private final AffinityTopologyVersion evtTopVer;

    /** Exchange id. */
    private final ServicesDeploymentExchangeId exchId;

    /** Remaining nodes to received single node assignments message. */
    @GridToStringInclude
    private final Set<UUID> remaining = new HashSet<>();

    /**
     * @param srvcsAssigns Services assignments.
     * @param ctx Kernal context.
     * @param evt Discovery event.
     * @param evtTopVer Topology version.
     */
    public ServicesDeploymentExchangeFutureTask(Map<String, GridServiceAssignments> srvcsAssigns,
        GridKernalContext ctx, DiscoveryEvent evt,
        AffinityTopologyVersion evtTopVer) {
        this.srvcsAssigns = srvcsAssigns;
        this.ctx = ctx;
        this.evt = evt;
        this.evtTopVer = evtTopVer;
        this.exchId = new ServicesDeploymentExchangeId(evt);
        this.log = ctx.log(getClass());
    }

    /** {@inheritDoc} */
    @Override public void init() throws IgniteCheckedException {
        try {
            if (log.isDebugEnabled()) {
                log.debug("Started services exchange future init: [exchId=" + exchangeId() +
                    ", locId=" + ctx.localNodeId() +
                    ", evt=" + evt + ']');
            }

            synchronized (mux) {
                for (ClusterNode node : ctx.discovery().allNodes()) {
                    if (ctx.discovery().alive(node))
                        remaining.add(node.id());
                }
            }

            if (evt instanceof DiscoveryCustomEvent) {
                DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)evt).customMessage();

                if (msg instanceof ServicesDeploymentRequestMessage)
                    onDeploymentRequest(evt.eventNode().id(), (ServicesDeploymentRequestMessage)msg, evtTopVer);
                else if (msg instanceof DynamicCacheChangeBatch)
                    onCacheStateChangeRequest((DynamicCacheChangeBatch)msg, evtTopVer);
                else if (msg instanceof CacheAffinityChangeMessage)
                    initFullReassignment(evtTopVer);
                else
                    complete(new IgniteIllegalStateException("Unexpected discovery custom message, msg=" + msg), true);
            }
            else {
                if (!srvcsAssigns.isEmpty()) {
                    switch (evt.type()) {
                        case EVT_NODE_JOINED:
                        case EVT_NODE_LEFT:
                        case EVT_NODE_FAILED:

                            initFullReassignment(evtTopVer);

                            break;

                        default:
                            complete(new IgniteIllegalStateException("Unexpected discovery event, evt=" + evt), true);

                            break;
                    }
                }
                else
                    complete(null, false);
            }

            if (log.isDebugEnabled()) {
                log.debug("Finished services exchange future init: [exchId=" + exchangeId() +
                    ", locId=" + ctx.localNodeId() + ']');
            }
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * @param snd Sender id.
     * @param req Services deployment request.
     * @param topVer Topology version.
     */
    private void onDeploymentRequest(UUID snd, ServicesDeploymentRequestMessage req, AffinityTopologyVersion topVer) {
        ServicesAssignmentsRequestMessage msg = new ServicesAssignmentsRequestMessage(snd, exchId,
            evtTopVer.topologyVersion());

        if (!req.servicesToDeploy().isEmpty()) {
            Collection<ServiceConfiguration> cfgs = req.servicesToDeploy();

            Collection<GridServiceAssignments> assigns = new ArrayList<>(cfgs.size());

            for (ServiceConfiguration cfg : cfgs) {
                GridServiceAssignments srvcAssigns = srvcsAssigns.get(cfg.getName());

                Throwable th = null;

                if (srvcAssigns != null && !srvcAssigns.configuration().equalsIgnoreNodeFilter(cfg)) {
                    th = new IgniteCheckedException("Failed to deploy service (service already exists with " +
                        "different configuration) [deployed=" + srvcAssigns.configuration() + ", new=" + cfg + ']');
                }
                else {
                    try {
                        srvcAssigns = ctx.service().reassign(cfg, snd, topVer);
                    }
                    catch (IgniteCheckedException e) {
                        th = new IgniteCheckedException("Failed to calculate assignment for service, cfg=" + cfg, e);
                    }

                    if (srvcAssigns != null && srvcAssigns.assigns().isEmpty())
                        th = new IgniteCheckedException("Failed to determine suitable nodes to deploy service, cfg=" + cfg);
                }

                if (th != null) {
                    log.error(th.getMessage(), th);

                    depErrors.put(cfg.getName(), th);

                    continue;
                }

                if (log.isDebugEnabled())
                    log.debug("Calculated service assignments: " + srvcAssigns);

                assigns.add(srvcAssigns);
            }

            msg.servicesToDeploy(assigns);
        }
        else
            msg.servicesToUndeploy(req.servicesToUndeploy());

        sendCustomEvent(msg);
    }

    /**
     * @param req Cacje state change request.
     * @param topVer Topology version;
     */
    private void onCacheStateChangeRequest(DynamicCacheChangeBatch req, AffinityTopologyVersion topVer) {
        Set<String> cacheToStop = new HashSet<>();

        for (DynamicCacheChangeRequest chReq : req.requests()) {
            if (chReq.stop())
                cacheToStop.add(chReq.cacheName());
        }

        Set<String> srvcsToUndeploy = new HashSet<>();

        srvcsAssigns.forEach((name, assigns) -> {
            if (cacheToStop.contains(assigns.cacheName()))
                srvcsToUndeploy.add(name);
        });

        ServicesAssignmentsRequestMessage msg = new ServicesAssignmentsRequestMessage(ctx.localNodeId(),
            exchId, topVer.topologyVersion());

        msg.servicesToUndeploy(srvcsToUndeploy);

        sendCustomEvent(msg);
    }

    /**
     * @param topVer Topology version.
     */
    private void initFullReassignment(AffinityTopologyVersion topVer) {
        ServicesAssignmentsRequestMessage msg = createReassignmentsRequest(topVer);

        sendCustomEvent(msg);
    }

    /** {@inheritDoc} */
    @Override public void onReceiveSingleMapMessage(ServicesSingleMapMessage msg) {
        synchronized (mux) {
            assert exchId.equals(msg.exchangeId()) : "Wrong messages exchange id, msg=" + msg;

            if (remaining.remove(msg.senderId())) {
                singleAssignsMsgs.put(msg.senderId(), msg);

                if (remaining.isEmpty())
                    onAllReceived();
            }
            else
                log.warning("Unexpected service assignments message received, msg=" + msg);
        }
    }

    /** {@inheritDoc} */
    @Override public void onReceiveFullMapMessage(ServicesFullMapMessage msg) {
        onDone();
    }

    /**
     * Creates full assignments message and send it across over discovery.
     */
    private void onAllReceived() {
        ServicesFullMapMessage msg = createFullAssignmentsMessage();

        sendCustomEvent(msg);
    }

    /**
     * Processes single assignments messages to build full assignments message.
     *
     * @return Services full assignments message.
     */
    private ServicesFullMapMessage createFullAssignmentsMessage() {
        synchronized (mux) {
            final Map<String, ServiceAssignmentsMap> assigns = new HashMap<>();

            final Map<String, Map<UUID, Integer>> fullAssigns = new HashMap<>();

            final Map<String, Collection<byte[]>> fullErrors = new HashMap<>();

            try {
                singleAssignsMsgs.forEach((nodeId, msg) -> {
                    msg.assigns().forEach((name, num) -> {
                        if (num != null && num != 0) {
                            Map<UUID, Integer> srvcAssigns = fullAssigns.computeIfAbsent(name, m -> new HashMap<>());

                            Map<UUID, Integer> expSrvcAssigns = expAssigns.get(name);

                            if (expSrvcAssigns != null) {
                                Integer expNum = expSrvcAssigns.get(nodeId);

                                if (expNum == null)
                                    num = 0;
                                else if (expNum < num)
                                    num = expNum;
                            }

                            srvcAssigns.put(nodeId, num);
                        }
                    });

                    msg.errors().forEach((name, err) -> {
                        Collection<byte[]> srvcErrors = fullErrors.computeIfAbsent(name, e -> new ArrayList<>());

                        srvcErrors.add(err);
                    });
                });

                fullAssigns.forEach((name, srvcAssigns) -> {
                    assigns.put(name, new ServiceAssignmentsMap(name, srvcAssigns, evt.topologyVersion()));
                });

                depErrors.forEach((name, err) -> {
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

            ServicesFullMapMessage msg = new ServicesFullMapMessage(ctx.localNodeId(), exchId, assigns);

            if (!fullErrors.isEmpty())
                msg.errors(fullErrors);

            return msg;
        }
    }

    /**
     * @param topVer Topology version.
     */
    private ServicesAssignmentsRequestMessage createReassignmentsRequest(AffinityTopologyVersion topVer) {
        final Map<String, Map<UUID, Integer>> fullAssigns = new HashMap<>();

        final Set<String> servicesToUndeploy = new HashSet<>();

        srvcsAssigns.forEach((name, old) -> {
            ServiceConfiguration cfg = old.configuration();

            GridServiceAssignments newAssigns = null;

            Throwable th = null;

            try {
                newAssigns = ctx.service().reassign(cfg, old.nodeId(), topVer);
            }
            catch (Throwable e) {
                log.error("Failed to recalculate assignments for service, cfg=" + cfg, e);

                th = e;
            }

            if (th == null && newAssigns != null && !newAssigns.assigns().isEmpty()) {
                if (log.isDebugEnabled())
                    log.debug("Calculated service assignments: " + newAssigns.assigns());

                fullAssigns.put(name, newAssigns.assigns());
            }
            else if (cfg.policy() == IGNORE) {
                Map<UUID, Integer> assigns = new HashMap<>();

                old.assigns().forEach((id, num) -> {
                    if (ctx.discovery().alive(id))
                        assigns.put(id, num);
                });

                if (!assigns.isEmpty())
                    fullAssigns.put(name, assigns);
                else
                    servicesToUndeploy.add(name);
            }
            else
                servicesToUndeploy.add(name);
        });

        expAssigns.putAll(fullAssigns);

        ServicesAssignmentsRequestMessage msg = new ServicesAssignmentsRequestMessage(ctx.localNodeId(),
            exchId, topVer.topologyVersion());

        msg.assignments(fullAssigns);

        if (!servicesToUndeploy.isEmpty())
            msg.servicesToUndeploy(servicesToUndeploy);

        return msg;
    }

    /**
     * Sends given message over discovery.
     *
     * @param msg Message to send.
     */
    private void sendCustomEvent(DiscoveryCustomMessage msg) {
        try {
            ctx.discovery().sendCustomEvent(msg);
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to send message across the ring, msg=" + msg, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onNodeLeft(UUID nodeId) {
        if (isDone() || remaining.isEmpty())
            return;

        remaining.remove(nodeId);

        if (remaining.isEmpty())
            onAllReceived();
    }

    /** {@inheritDoc} */
    @Override public ServicesDeploymentExchangeId exchangeId() {
        return exchId;
    }

    /** {@inheritDoc} */
    @Override public void complete(@Nullable Throwable err, boolean cancel) {
        onDone(null, err, cancel);
    }

    /** {@inheritDoc} */
    @Override public void waitForComplete(long timeout) throws IgniteCheckedException {
        get(timeout);
    }

    /**
     * @return Cause discovery event.
     */
    public DiscoveryEvent event() {
        return evt;
    }

    /** {@inheritDoc} */
    @Override public Set<UUID> remaining() {
        return Collections.unmodifiableSet(remaining);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ServicesDeploymentExchangeFutureTask fut1 = (ServicesDeploymentExchangeFutureTask)o;

        return exchId.equals(fut1.exchId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return exchId.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServicesDeploymentExchangeFutureTask.class, this);
    }
}
