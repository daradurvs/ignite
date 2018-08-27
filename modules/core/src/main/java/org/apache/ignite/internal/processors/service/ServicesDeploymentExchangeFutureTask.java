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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
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
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.services.ServiceConfiguration;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 * Services deployment exchange future.
 */
public class ServicesDeploymentExchangeFutureTask extends GridFutureAdapter<Object> implements ServicesDeploymentExchangeTask, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Single service messages to process. */
    @GridToStringInclude
    private final Map<UUID, ServicesSingleMapMessage> singleMapMsgs = new HashMap<>();

    /** Expected services assignments. */
    private final Map<IgniteUuid, Map<UUID, Integer>> expDeps = new HashMap<>();

    /** Deployment errors. */
    @GridToStringInclude
    private final Map<IgniteUuid, Throwable> depErrors = new HashMap<>();

    /** Remaining nodes to received single node assignments message. */
    @GridToStringInclude
    private final Set<UUID> remaining = new HashSet<>();

    /** Mutex. */
    private final Object mux = new Object();

    /** Discovery event. */
    @GridToStringInclude
    private DiscoveryEvent evt;

    /** Cause event topology version. */
    private AffinityTopologyVersion evtTopVer;

    /** Exchange id. */
    private ServicesDeploymentExchangeId exchId;

    /** Kernal context. */
    private GridKernalContext ctx;

    /** Services deployments. */
    private Map<IgniteUuid, GridServiceDeployment> srvcsDeps;

    /** Services topologies. */
    private Map<IgniteUuid, HashMap<UUID, Integer>> srvcsTops;

    /** Logger. */
    private IgniteLogger log;

    /**
     * Empty constructor for marshalling purposes.
     */
    public ServicesDeploymentExchangeFutureTask() {
    }

    /**
     * @param evt Discovery event.
     * @param evtTopVer Topology version.
     */
    public ServicesDeploymentExchangeFutureTask(DiscoveryEvent evt, AffinityTopologyVersion evtTopVer,
        ServicesDeploymentExchangeId exchId) {
        this.evt = evt;
        this.evtTopVer = evtTopVer;
        this.exchId = exchId;
    }

    /**
     * @param ctx Kernal context.
     */
    private void init0(GridKernalContext ctx) {
        this.ctx = ctx;
        this.log = ctx.log(getClass());
        this.srvcsDeps = ctx.service().deployments();
        this.srvcsTops = ctx.service().servicesTopologies();
    }

    /** {@inheritDoc} */
    @Override public void init(GridKernalContext ctx) throws IgniteCheckedException {
        if (isDone())
            return;

        try {
            init0(ctx);

            if (log.isDebugEnabled()) {
                log.debug("Started services exchange future init: [exchId=" + exchangeId() +
                    ", locId=" + ctx.localNodeId() +
                    ", evt=" + evt + ']');
            }

            synchronized (mux) {
                for (ClusterNode node : ctx.discovery().nodes(evtTopVer)) {
                    if (ctx.discovery().alive(node))
                        remaining.add(node.id());
                }
            }

            if (evt instanceof DiscoveryCustomEvent) {
                DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)evt).customMessage();

                if (msg instanceof DynamicServicesChangeRequestBatchMessage)
                    onServiceChangeRequest((DynamicServicesChangeRequestBatchMessage)msg, evtTopVer);
                else if (msg instanceof DynamicCacheChangeBatch)
                    onCacheStateChangeRequest((DynamicCacheChangeBatch)msg);
                else if (msg instanceof CacheAffinityChangeMessage)
                    initFullReassignment(evtTopVer);
                else
                    complete(new IgniteIllegalStateException("Unexpected discovery custom message, msg=" + msg), true);
            }
            else {
                if (!srvcsDeps.isEmpty()) {
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
            log.error("Error occurred during deployment task initialization, err=" + e.getMessage(), e);

            throw new IgniteCheckedException(e);
        }
    }

    /**
     * @param batch Set of requests to change service.
     * @param topVer Topology version.
     */
    private void onServiceChangeRequest(DynamicServicesChangeRequestBatchMessage batch,
        AffinityTopologyVersion topVer) {
        Map<IgniteUuid, Map<UUID, Integer>> srvcsToDeploy = new HashMap<>();

        Collection<IgniteUuid> srvcsToUndeploy = new ArrayList<>();

        for (DynamicServiceChangeRequest req : batch.requests()) {
            IgniteUuid srvcId = req.serviceId();

            if (req.deploy()) {
                ServiceConfiguration cfg = req.configuration();

                Map<UUID, Integer> srvcTop = srvcsTops.get(srvcId);

                Throwable th = null;

                if (srvcTop != null) { // In case of a collision of IgniteUuid.randomUuid() (almost impossible case)
                    th = new IgniteCheckedException("Failed to deploy service. Service with generated id already exists" +
                        ", srvcTop=" + srvcTop);
                }

                GridServiceDeployment oldSrvcDep = null;
                IgniteUuid oldSrvcId = null;

                for (Map.Entry<IgniteUuid, GridServiceDeployment> e : srvcsDeps.entrySet()) {
                    GridServiceDeployment dep = e.getValue();

                    if (dep.configuration().getName().equals(cfg.getName())) {
                        oldSrvcDep = dep;
                        oldSrvcId = e.getKey();

                        break;
                    }
                }

                if (oldSrvcDep != null && !oldSrvcDep.configuration().equalsIgnoreNodeFilter(cfg)) {
                    th = new IgniteCheckedException("Failed to deploy service (service already exists with " +
                        "different configuration) [deployed=" + oldSrvcDep.configuration() + ", new=" + cfg + ']');
                }
                else {
                    try {
                        if (oldSrvcId != null)
                            srvcId = oldSrvcId;

                        srvcTop = ctx.service().reassign(cfg, topVer);
                    }
                    catch (IgniteCheckedException e) {
                        th = new IgniteCheckedException("Failed to calculate assignment for service, cfg=" + cfg, e);
                    }

                    if (srvcTop != null && srvcTop.isEmpty())
                        th = new IgniteCheckedException("Failed to determine suitable nodes to deploy service, cfg=" + cfg);
                }

                if (th != null) {
                    log.error(th.getMessage(), th);

                    depErrors.put(req.serviceId(), th);

                    continue;
                }

                if (log.isDebugEnabled()) {
                    log.debug("Calculated service assignments" +
                        ", srvcId=" + oldSrvcDep +
                        ", top=" + srvcTop);
                }

                srvcsToDeploy.put(srvcId, srvcTop);
            }
            else if (req.undeploy())
                srvcsToUndeploy.add(srvcId);
        }

        ServicesAssignmentsRequestMessage msg = new ServicesAssignmentsRequestMessage(exchId);

        if (!srvcsToDeploy.isEmpty())
            msg.servicesToDeploy(srvcsToDeploy);

        if (!srvcsToUndeploy.isEmpty())
            msg.servicesToUndeploy(srvcsToUndeploy);

        sendCustomEvent(msg);
    }

    /**
     * @param req Cacje state change request.
     */
    private void onCacheStateChangeRequest(DynamicCacheChangeBatch req) {
        Set<String> cacheToStop = new HashSet<>();

        for (DynamicCacheChangeRequest chReq : req.requests()) {
            if (chReq.stop())
                cacheToStop.add(chReq.cacheName());
        }

        Set<IgniteUuid> srvcsToUndeploy = new HashSet<>();

        srvcsDeps.forEach((id, dep) -> {
            if (cacheToStop.contains(dep.configuration().getCacheName()))
                srvcsToUndeploy.add(id);
        });

        ServicesAssignmentsRequestMessage msg = new ServicesAssignmentsRequestMessage(exchId);

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
    @Override public void onReceiveSingleMapMessage(UUID snd, ServicesSingleMapMessage msg) {
        assert exchId.equals(msg.exchangeId()) : "Wrong messages exchange id, msg=" + msg;

        synchronized (mux) {
            if (remaining.remove(snd)) {
                singleMapMsgs.put(snd, msg);

                if (remaining.isEmpty())
                    onAllReceived();
            }
            else
                log.warning("Unexpected service assignments message received, msg=" + msg);
        }
    }

    /** {@inheritDoc} */
    @Override public void onReceiveFullMapMessage(UUID snd, ServicesFullMapMessage msg) {
        assert exchId.equals(msg.exchangeId()) : "Wrong messages exchange id, msg=" + msg;

        complete(null, false);
    }

    /**
     * Creates full assignments message and send it across over discovery.
     */
    private void onAllReceived() {
        ServicesFullMapMessage msg = createFullMapMessage();

        sendCustomEvent(msg);
    }

    /**
     * Processes single map messages to build full map message.
     *
     * @return Services full map message.
     */
    private ServicesFullMapMessage createFullMapMessage() {
        final Map<IgniteUuid, Map<UUID, ServiceSingleDeploymentsResults>> results = new HashMap<>();

        singleMapMsgs.forEach((nodeId, msg) -> {
            msg.results().forEach((srvcId, res) -> {
                Map<UUID, ServiceSingleDeploymentsResults> depResults = results.computeIfAbsent(srvcId, r -> new HashMap<>());

                int num = res.count();

                if (num != 0) {
                    Map<UUID, Integer> expSrvcTop = expDeps.get(srvcId);

                    if (expSrvcTop != null) {
                        Integer expNum = expSrvcTop.get(nodeId);

                        if (expNum == null)
                            num = 0;
                        else if (expNum < num)
                            num = expNum;
                    }
                }

                ServiceSingleDeploymentsResults singleDepRes = new ServiceSingleDeploymentsResults(num);

                if (!res.errors().isEmpty())
                    singleDepRes.errors(res.errors());

                depResults.put(nodeId, singleDepRes);
            });
        });

        depErrors.forEach((id, err) -> {
            byte[] arr = null;

            try {
                arr = U.marshal(ctx, err);
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to marshal reassignments error.", e);
            }

            if (arr != null) {
                Collection<byte[]> srvcErrors = new ArrayList<>(1);

                srvcErrors.add(arr);

                Map<UUID, ServiceSingleDeploymentsResults> depResults = results.computeIfAbsent(id, r -> new HashMap<>());

                ServiceSingleDeploymentsResults singleDepRes = depResults.computeIfAbsent(
                    ctx.localNodeId(), r -> new ServiceSingleDeploymentsResults(0));

                if (singleDepRes.errors().isEmpty())
                    singleDepRes.errors(srvcErrors);
                else
                    singleDepRes.errors().addAll(srvcErrors);
            }
        });

        final Collection<ServiceFullDeploymentsResults> fullResults = new ArrayList<>();

        results.forEach((srvcId, dep) -> {
            ServiceFullDeploymentsResults res = new ServiceFullDeploymentsResults(srvcId, dep);

            fullResults.add(res);
        });

        return new ServicesFullMapMessage(exchId, fullResults);
    }

    /**
     * @param topVer Topology version.
     */
    private ServicesAssignmentsRequestMessage createReassignmentsRequest(AffinityTopologyVersion topVer) {
        final Map<IgniteUuid, Map<UUID, Integer>> fullTops = new HashMap<>();

        final Set<IgniteUuid> servicesToUndeploy = new HashSet<>();

        srvcsDeps.forEach((srvcId, old) -> {
            ServiceConfiguration cfg = old.configuration();

            Map<UUID, Integer> top = null;

            Throwable th = null;

            try {
                top = ctx.service().reassign(cfg, topVer);
            }
            catch (Throwable e) {
                log.error("Failed to recalculate assignments for service, cfg=" + cfg, e);

                th = e;
            }

            if (th == null && !top.isEmpty()) {
                if (log.isDebugEnabled())
                    log.debug("Calculated service assignments: " + top);

                fullTops.put(srvcId, top);
            }
            else {
                if (th != null) {
                    th = new IgniteException("Failed to determine suitable nodes to deploy service" +
                        ", srvcId=" + srvcId +
                        ", cfg=" + cfg);
                }

                depErrors.put(srvcId, th);

                servicesToUndeploy.add(srvcId);
            }
        });

        expDeps.putAll(fullTops);

        ServicesAssignmentsRequestMessage msg = new ServicesAssignmentsRequestMessage(exchId);

        if (!fullTops.isEmpty())
            msg.servicesToDeploy(fullTops);

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

        synchronized (mux) {
            remaining.remove(nodeId);

            if (remaining.isEmpty())
                onAllReceived();
        }
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
    @Override public boolean isComplete() {
        return isDone();
    }

    /** {@inheritDoc} */
    @Override public void waitForComplete(long timeout) throws IgniteCheckedException {
        get(timeout);
    }

    /** {@inheritDoc} */
    @Override public DiscoveryEvent event() {
        return evt;
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion topologyVersion() {
        return evtTopVer;
    }

    /** {@inheritDoc} */
    @Override public Set<UUID> remaining() {
        return Collections.unmodifiableSet(remaining);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(evt);
        out.writeObject(evtTopVer);
        out.writeObject(exchId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        evt = (DiscoveryEvent)in.readObject();
        evtTopVer = (AffinityTopologyVersion)in.readObject();
        exchId = (ServicesDeploymentExchangeId)in.readObject();
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
