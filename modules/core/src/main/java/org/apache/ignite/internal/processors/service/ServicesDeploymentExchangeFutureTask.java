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
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
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
import org.apache.ignite.lang.IgniteInClosure;
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

    /** Indicates that cause discovery event is set. */
    private final CountDownLatch evtLatch = new CountDownLatch(1);

    /** Task's initialization future. */
    private final GridFutureAdapter<?> initFut = new GridFutureAdapter<>();

    /** Single service messages to process. */
    @GridToStringInclude
    private final Map<UUID, ServicesSingleMapMessage> singleMapMsgs = new HashMap<>();

    /** Expected services assignments. */
    private final Map<IgniteUuid, Map<UUID, Integer>> expDeps = new HashMap<>();

    /** Deployment errors. */
    @GridToStringInclude
    private final Map<IgniteUuid, Collection<Throwable>> depErrors = new HashMap<>();

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

    /** If local node coordinator. */
    private volatile boolean crd;

    private volatile UUID crdNode;

    /**
     * Empty constructor for marshalling purposes.
     */
    public ServicesDeploymentExchangeFutureTask() {
    }

    /**
     * @param evt Discovery event.
     * @param evtTopVer Topology version.
     */
    public ServicesDeploymentExchangeFutureTask(GridKernalContext ctx, DiscoveryEvent evt,
        AffinityTopologyVersion evtTopVer,
        ServicesDeploymentExchangeId exchId) {
        this.evt = evt;
        this.evtTopVer = evtTopVer;
        this.exchId = exchId;
        this.ctx = ctx;
        this.log = ctx.log(getClass());
        this.srvcsDeps = ctx.service().deployments();
        this.srvcsTops = ctx.service().servicesTopologies();
    }

    /**
     * @param ctx Kernal context.
     * @param exchId Service deployment exchange id.
     */
    public ServicesDeploymentExchangeFutureTask(GridKernalContext ctx, ServicesDeploymentExchangeId exchId) {
        this.exchId = exchId;
        this.ctx = ctx;
        this.log = ctx.log(getClass());
        this.srvcsDeps = ctx.service().deployments();
        this.srvcsTops = ctx.service().servicesTopologies();
    }

    /** {@inheritDoc} */
    @Override public void event(DiscoveryEvent evt, AffinityTopologyVersion evtTopVer) {
        if (evtLatch.getCount() < 1)
            return;

        assert evt != null && evtTopVer != null;

        this.evt = evt;
        this.evtTopVer = evtTopVer;

        evtLatch.countDown();
    }

    /** {@inheritDoc} */
    @Override public void init() throws IgniteCheckedException {
        if (isDone())
            return;

        try {
            U.await(evtLatch);

            crd = ctx.service().isLocalNodeCoordinator();
            crdNode = ctx.service().coordinator().id();

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
                // TODO
//                if (evt.type() == EVT_NODE_JOINED)
//                    remaining.remove(evt.eventNode().id());
//
//                if (!srvcsDeps.isEmpty()) {
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
//                }
//                else
//                    complete(null, false);
            }

            if (log.isDebugEnabled()) {
                log.debug("Finished services exchange future init: [exchId=" + exchangeId() +
                    ", locId=" + ctx.localNodeId() + ']');
            }

            initFut.onDone();
        }
        catch (Exception e) {
            log.error("Error occurred during deployment task initialization, err=" + e.getMessage(), e);

            initFut.onDone(e);

            throw new IgniteCheckedException(e);
        }
    }

    /**
     * @param batch Set of requests to change service.
     * @param topVer Topology version.
     */
    private void onServiceChangeRequest(DynamicServicesChangeRequestBatchMessage batch,
        AffinityTopologyVersion topVer) throws IgniteCheckedException {
//        Map<IgniteUuid, Map<UUID, Integer>> srvcsToDeploy = new HashMap<>();

//        Collection<IgniteUuid> srvcsToUndeploy = new ArrayList<>();

//        final Map<IgniteUuid, Collection<Throwable>> errors = new HashMap<>();

        for (DynamicServiceChangeRequest req : batch.requests()) {
            IgniteUuid srvcId = req.serviceId();

            if (req.undeploy())
                ctx.service().undeploy(srvcId);
            else if (req.deploy()) {
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

                        srvcTop = ctx.service().reassign(srvcId, cfg, topVer);
                    }
                    catch (IgniteCheckedException e) {
                        th = new IgniteCheckedException("Failed to calculate assignment for service, cfg=" + cfg, e);
                    }

                    if (srvcTop != null && srvcTop.isEmpty())
                        th = new IgniteCheckedException("Failed to determine suitable nodes to deploy service, cfg=" + cfg);
                }

                if (th != null) {
                    log.error(th.getMessage(), th);

                    Collection<Throwable> errors = depErrors.computeIfAbsent(req.serviceId(), e -> new ArrayList<>());

                    errors.add(th);

                    continue;
                }

                if (log.isDebugEnabled()) {
                    log.debug("Calculated service assignments" +
                        ", srvcId=" + srvcId +
                        ", top=" + srvcTop);
                }

                GridServiceDeployment dep = srvcsDeps.get(srvcId);

                if (dep == null) {
                    dep = new GridServiceDeployment(evt.eventNode().id(), cfg);

                    srvcsDeps.put(srvcId, dep);
                }

                ctx.service().deployIfNeeded(srvcId, srvcTop, depErrors);
            }
        }

        ctx.service().createAndSendSingleMapMessage(exchId, depErrors);
    }

    /**
     * @param req Cacje state change request.
     */
    private void onCacheStateChangeRequest(DynamicCacheChangeBatch req) throws IgniteCheckedException {
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

        for (IgniteUuid srvcId : srvcsToUndeploy)
            ctx.service().undeploy(srvcId);

        ctx.service().createAndSendSingleMapMessage(exchId, depErrors);
    }

    /**
     * @param topVer Topology version.
     */
    private void initFullReassignment(AffinityTopologyVersion topVer) throws IgniteCheckedException {
        final Map<IgniteUuid, Map<UUID, Integer>> fullTops = new HashMap<>();

        final Set<IgniteUuid> servicesToUndeploy = new HashSet<>();

        srvcsDeps.forEach((srvcId, old) -> {
            ServiceConfiguration cfg = old.configuration();

            Map<UUID, Integer> top = null;

            Throwable th = null;

            try {
                top = ctx.service().reassign(srvcId, cfg, topVer);
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

                Collection<Throwable> errors = depErrors.computeIfAbsent(srvcId, e -> new ArrayList<>());

                errors.add(th);

                servicesToUndeploy.add(srvcId);
            }
        });

        expDeps.putAll(fullTops);

        for (IgniteUuid srvcId : servicesToUndeploy)
            ctx.service().undeploy(srvcId);

        fullTops.forEach((srvcId, top) -> {
            ctx.service().deployIfNeeded(srvcId, top, depErrors);
        });

        ctx.service().createAndSendSingleMapMessage(exchId, depErrors);
//        ServicesAssignmentsRequestMessage msg = new ServicesAssignmentsRequestMessage(exchId);
//
//        if (!fullTops.isEmpty())
//            msg.servicesToDeploy(fullTops);
//
//        if (!servicesToUndeploy.isEmpty())
//            msg.servicesToUndeploy(servicesToUndeploy);
//
//        sendCustomEvent(msg);
    }

    /** {@inheritDoc} */
    @Override public void onReceiveSingleMapMessage(UUID snd, ServicesSingleMapMessage msg) {
        assert exchId.equals(msg.exchangeId()) : "Wrong messages exchange id, msg=" + msg;

        initFut.listen(new IgniteInClosure<IgniteInternalFuture<?>>() {
            @Override public void apply(IgniteInternalFuture<?> fut) {

                try {
                    fut.get();
                }
                catch (IgniteCheckedException e) {
                    log.error("Failed to initialize task: " + this, e);

                    return;
                }

                onReceiveSingleMapMessage0(snd, msg);
            }
        });
    }

    /**
     * @param snd Sender id.
     * @param msg Service single map message.
     */
    public void onReceiveSingleMapMessage0(UUID snd, ServicesSingleMapMessage msg) {
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
        ServicesFullMapMessage msg = createFullMapMessageUsingSingleDeploymentsResults();

        sendCustomEvent(msg);
    }

    /**
     * Processes single map messages to build full map message.
     *
     * @return Services full map message.
     */
    private ServicesFullMapMessage createFullMapMessageUsingSingleDeploymentsResults() {
        final Map<IgniteUuid, Map<UUID, ServiceSingleDeploymentsResults>> results = new HashMap<>();

        singleMapMsgs.forEach((nodeId, msg) -> {
            msg.results().forEach((srvcId, res) -> {
                Map<UUID, ServiceSingleDeploymentsResults> depResults = results.computeIfAbsent(srvcId, r -> new HashMap<>());

                int cnt = res.count();

                if (cnt != 0) {
                    Map<UUID, Integer> expSrvcTop = expDeps.get(srvcId);

                    if (expSrvcTop != null) {
                        Integer expCnt = expSrvcTop.get(nodeId);

                        if (expCnt == null)
                            cnt = 0;
                        else if (expCnt < cnt)
                            cnt = expCnt;
                    }
                }

                ServiceSingleDeploymentsResults singleDepRes = new ServiceSingleDeploymentsResults(cnt);

                if (!res.errors().isEmpty())
                    singleDepRes.errors(res.errors());

                depResults.put(nodeId, singleDepRes);
            });
        });

        return createFullMapMessage0(results);
    }

    /**
     * Processes services topology messages to build full map message.
     *
     * @return Services full map message.
     */
    private ServicesFullMapMessage createFullMapMessageUsingServicesTopology() {
        final Map<IgniteUuid, Map<UUID, ServiceSingleDeploymentsResults>> results = new HashMap<>();

        srvcsTops.forEach((srvcId, top) -> {
            Map<UUID, ServiceSingleDeploymentsResults> depResults = new HashMap<>();

            top.forEach((nodeId, cnt) -> {
                depResults.put(nodeId, new ServiceSingleDeploymentsResults(cnt));
            });

            results.put(srvcId, depResults);
        });

        return createFullMapMessage0(results);
    }

    /**
     * @param results Services per node single deployments results.
     * @return Services full map message.
     */
    private ServicesFullMapMessage createFullMapMessage0(
        final Map<IgniteUuid, Map<UUID, ServiceSingleDeploymentsResults>> results) {
        depErrors.forEach((id, arr) -> {
            Collection<byte[]> srvcErrors = new ArrayList<>(arr.size());

            for (Throwable th : arr) {
                byte[] err = null;

                try {
                    err = U.marshal(ctx, th);
                }
                catch (IgniteCheckedException e) {
                    log.error("Failed to marshal reassignments error.", e);
                }

                if (err != null)
                    srvcErrors.add(err);
            }

            if (!srvcErrors.isEmpty()) {
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
        initFut.listen(new IgniteInClosure<IgniteInternalFuture<?>>() {
            @Override public void apply(IgniteInternalFuture<?> fut) {

                try {
                    fut.get();
                }
                catch (IgniteCheckedException e) {
                    log.error("Failed to initialize task: " + this, e);

                    return;
                }

                onNodeLeft0(nodeId);
            }
        });
    }

    /**
     * @param nodeId Node id.
     */
    private void onNodeLeft0(UUID nodeId) {
        if (isDone())
            return;
        synchronized (mux) {

//            final boolean curCrd = ctx.service().isLocalNodeCoordinator();
//
//            final boolean crdChanged = crd != curCrd;
//            ClusterNode q = ctx.discovery().node(nodeId);
            final boolean crdChanged = nodeId.equals(crdNode);

            if (crdChanged) {
                try {
//                    crd = curCrd;
                    crdNode = nodeId;

                    remaining.remove(nodeId);

                    ctx.service().createAndSendSingleMapMessage(exchId, depErrors);
                }
                catch (IgniteCheckedException e) {
                    e.printStackTrace();
                }
            }
            else if (ctx.localNodeId().equals(crdNode)) {

                remaining.remove(nodeId);

                if (remaining.isEmpty())
                    onAllReceived();
            }
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
        return S.toString(ServicesDeploymentExchangeFutureTask.class, this, "locNodeId", ctx.localNodeId());
    }
}
