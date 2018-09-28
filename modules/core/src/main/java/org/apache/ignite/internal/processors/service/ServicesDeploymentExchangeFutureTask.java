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
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeRequest;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDeploymentFailuresPolicy;
import org.apache.ignite.services.ServiceDescriptor;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridTopic.TOPIC_SERVICES;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SERVICE_POOL;

/**
 * Services deployment exchange task implementation based on {@link GridFutureAdapter}.
 */
public class ServicesDeploymentExchangeFutureTask extends GridFutureAdapter<Object>
    implements ServicesDeploymentExchangeTask, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Task's completion of initialization future. */
    private final GridFutureAdapter<?> initTaskFut = new GridFutureAdapter<>();

    /** Task's completion of remaining nodes ids initialization future. */
    private final GridFutureAdapter<?> initCrdFut = new GridFutureAdapter<>();

    /** Coordinator initialization actions mutex. */
    private final Object initCrdMux = new Object();

    /** Single service messages to process. */
    @GridToStringInclude
    private final Map<UUID, ServicesSingleMapMessage> singleMapMsgs = new HashMap<>();

    /** Expected services assignments. */
    @GridToStringExclude
    private final Map<IgniteUuid, Map<UUID, Integer>> expDeps = new HashMap<>();

    /** Deployment errors. */
    @GridToStringExclude
    private final Map<IgniteUuid, Collection<Throwable>> depErrors = new HashMap<>();

    /** Remaining nodes to received single node assignments message. */
    @GridToStringInclude
    private final Set<UUID> remaining = new HashSet<>();

    /** Exchange id. */
    @GridToStringInclude
    private ServicesDeploymentExchangeId exchId;

    /** Discovery event. */
    @GridToStringInclude
    private DiscoveryEvent evt;

    /** Cause event topology version. */
    @GridToStringInclude
    private AffinityTopologyVersion evtTopVer;

    /** Kernal context. */
    private GridKernalContext ctx;

    /** Logger. */
    private IgniteLogger log;

    /** Coordinator node id. */
    private volatile UUID crdId;

    /** Topology version of local join. */
    private AffinityTopologyVersion locJoinTopVer;

    /**
     * Empty constructor for marshalling purposes.
     */
    public ServicesDeploymentExchangeFutureTask() {
    }

    /**
     * @param exchId Service deployment exchange id.
     */
    public ServicesDeploymentExchangeFutureTask(ServicesDeploymentExchangeId exchId) {
        this.exchId = exchId;
    }

    /** {@inheritDoc} */
    @Override public void event(DiscoveryEvent evt, AffinityTopologyVersion evtTopVer) {
        assert evt != null && evtTopVer != null;

        this.evt = evt;
        this.evtTopVer = evtTopVer;
    }

    /** {@inheritDoc} */
    @Override public void init(GridKernalContext ctx) throws IgniteCheckedException {
        if (isCompleted() || initTaskFut.isDone())
            return;

        assert evt != null;
        assert evtTopVer != null;

        this.ctx = ctx;
        this.log = ctx.log(getClass());
        this.locJoinTopVer = ctx.discovery().localJoin().joinTopologyVersion();

        if (log.isDebugEnabled()) {
            log.debug("Started services exchange future init: [exchId=" + exchangeId() +
                ", locId=" + this.ctx.localNodeId() +
                ", evt=" + evt + ']');
        }

        try {
            ClusterNode crd = this.ctx.service().coordinator();

            if (crd != null) {
                crdId = crd.id();

                if (crd.isLocal())
                    initCoordinator(evtTopVer);
            }

            switch (evt.type()) {
                case EVT_DISCOVERY_CUSTOM_EVT:
                    DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)evt).customMessage();

                    if (msg instanceof ChangeGlobalStateMessage)
                        onChangeGlobalStateMessage((ChangeGlobalStateMessage)msg, evtTopVer);
                    else if (msg instanceof DynamicServicesChangeRequestBatchMessage)
                        onServiceChangeRequest((DynamicServicesChangeRequestBatchMessage)msg, evtTopVer);
                    else if (!lessThenLocalJoin(evtTopVer)) {
                        if (msg instanceof DynamicCacheChangeBatch)
                            onCacheStateChangeRequest((DynamicCacheChangeBatch)msg);
                        else if (msg instanceof CacheAffinityChangeMessage)
                            onCacheAffinityChangeMessage(evtTopVer);
                        else
                            complete(new IgniteIllegalStateException("Unexpected type of discovery custom message" +
                                ", msg=" + msg), true);
                    }

                    break;

                case EVT_NODE_JOINED:
                case EVT_NODE_LEFT:
                case EVT_NODE_FAILED:

                    if (!lessThenLocalJoin(evtTopVer))
                        initReassignment(this.ctx.service().services().keySet(), evtTopVer);

                    break;

                default:
                    complete(new IgniteIllegalStateException("Unexpected type of discovery event" +
                        ", evt=" + evt), true);

                    break;
            }

            if (log.isDebugEnabled()) {
                log.debug("Finished services exchange future init: [exchId=" + exchangeId() +
                    ", locId=" + this.ctx.localNodeId() + ']');
            }
        }
        catch (Exception e) {
            log.error("Error occurred during deployment task initialization, err=" + e.getMessage(), e);

            initTaskFut.onDone(e);

            throw new IgniteCheckedException(e);
        }
        finally {
            if (!initTaskFut.isDone())
                initTaskFut.onDone();
        }
    }

    /**
     * Prepares the coordinator to manage exchange.
     *
     * @param topVer Topology version to initialize {@link #remaining} collection.
     */
    private void initCoordinator(AffinityTopologyVersion topVer) {
        synchronized (initCrdMux) {
            if (initCrdFut.isDone())
                return;

            try {
                for (ClusterNode node : ctx.discovery().nodes(topVer)) {
                    if (ctx.discovery().alive(node))
                        remaining.add(node.id());
                }
            }
            catch (Exception e) {
                log.error("Error occurred while initialization of remaining collection.", e);

                initCrdFut.onDone(e);
            }
            finally {
                if (!initCrdFut.isDone())
                    initCrdFut.onDone();
            }
        }
    }

    /**
     * @param req Change cluster state message.
     * @param topVer Topology version.
     * @throws IgniteCheckedException In case of an error.
     */
    private void onChangeGlobalStateMessage(ChangeGlobalStateMessage req,
        AffinityTopologyVersion topVer) throws IgniteCheckedException {
        if (req.activate()) {
            ctx.service().onActivate(ctx);

            initReassignment(ctx.service().services().keySet(), topVer);
        }
        else {
            ctx.service().onDeActivate(ctx);

            complete(null, false);
        }
    }

    /**
     * @param batch Set of requests to change service.
     * @param topVer Topology version.
     */
    private void onServiceChangeRequest(DynamicServicesChangeRequestBatchMessage batch,
        AffinityTopologyVersion topVer) {
        final Map<IgniteUuid, ServiceInfo> srvcsDescs = ctx.service().services();

        final Map<IgniteUuid, Map<UUID, Integer>> srvcsToDeploy = new HashMap<>();

        final Set<IgniteUuid> srvcsToUndeploy = new HashSet<>();

        for (DynamicServiceChangeRequest req : batch.requests()) {
            IgniteUuid srvcId = req.serviceId();

            if (req.undeploy())
                srvcsToUndeploy.add(srvcId);
            else if (req.deploy()) {
                ServiceConfiguration cfg = req.configuration();

                ServiceDescriptor srvcDesc = srvcsDescs.get(srvcId);

                Map<UUID, Integer> srvcTop = srvcDesc != null ? srvcDesc.topologySnapshot() : null;

                Throwable th = null;

                if (srvcTop != null) { // In case of a collision of IgniteUuid.randomUuid() (almost impossible case)
                    th = new IgniteCheckedException("Failed to deploy service. Service with generated id already exists" +
                        ", srvcTop=" + srvcTop);
                }
                else {
                    ServiceInfo oldSrvcDesc = null;
                    IgniteUuid oldSrvcId = null;

                    for (Map.Entry<IgniteUuid, ServiceInfo> e : srvcsDescs.entrySet()) {
                        ServiceInfo desc = e.getValue();

                        if (desc.configuration().getName().equals(cfg.getName())) {
                            oldSrvcDesc = desc;
                            oldSrvcId = e.getKey();

                            break;
                        }
                    }

                    if (oldSrvcDesc != null && !oldSrvcDesc.configuration().equalsIgnoreNodeFilter(cfg)) {
                        th = new IgniteCheckedException("Failed to deploy service (service already exists with " +
                            "different configuration) [deployed=" + oldSrvcDesc.configuration() + ", new=" + cfg + ']');
                    }
                    else {
                        try {
                            if (oldSrvcId != null)
                                srvcId = oldSrvcId;

                            srvcTop = reassign(srvcId, cfg, topVer);
                        }
                        catch (IgniteCheckedException e) {
                            th = e;
                        }

                        if (srvcTop != null && srvcTop.isEmpty() && !lessThenLocalJoin(topVer))
                            th = new IgniteCheckedException("Failed to determine suitable nodes to deploy service, cfg=" + cfg);
                    }
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

                srvcsDescs.computeIfAbsent(srvcId, dep -> new ServiceInfo(evt.eventNode().id(), cfg));

                srvcsToDeploy.put(srvcId, srvcTop);
            }
        }

        if (!lessThenLocalJoin(topVer))
            changeServices(srvcsToDeploy, srvcsToUndeploy);
    }

    /**
     * @param topVer Topology version.
     */
    private void onCacheAffinityChangeMessage(AffinityTopologyVersion topVer) {
        Set<IgniteUuid> toReassign = new HashSet<>();

        ctx.service().services().forEach((srvcId, dep) -> {
            if (dep.configuration().getCacheName() != null)
                toReassign.add(srvcId);
        });

        if (toReassign.isEmpty()) {
            complete(null, false);

            return;
        }

        initReassignment(toReassign, topVer);
    }

    /**
     * @param req Cache state change request.
     */
    private void onCacheStateChangeRequest(DynamicCacheChangeBatch req) {
        Set<String> cachesToStop = new HashSet<>();

        for (DynamicCacheChangeRequest chReq : req.requests()) {
            if (chReq.stop())
                cachesToStop.add(chReq.cacheName());
        }

        Set<IgniteUuid> srvcsToUndeploy = new HashSet<>();

        ctx.service().services().forEach((id, dep) -> {
            if (cachesToStop.contains(dep.configuration().getCacheName()))
                srvcsToUndeploy.add(id);
        });

        if (srvcsToUndeploy.isEmpty()) {
            complete(null, false);

            return;
        }

        changeServices(Collections.emptyMap(), srvcsToUndeploy);
    }

    /**
     * @param toReassign Services to reassign.
     * @param topVer Topology version.
     */
    private void initReassignment(Set<IgniteUuid> toReassign, AffinityTopologyVersion topVer) {
        final Map<IgniteUuid, ServiceInfo> srvcsDescs = ctx.service().services();

        final Map<IgniteUuid, Map<UUID, Integer>> srvcsToDeploy = new HashMap<>();

        final Set<IgniteUuid> srvcsToUndeploy = new HashSet<>();

        for (IgniteUuid srvcId : toReassign) {
            ServiceInfo desc = srvcsDescs.get(srvcId);

            ServiceConfiguration cfg = desc.configuration();

            Map<UUID, Integer> top = null;

            Throwable th = null;

            try {
                top = reassign(srvcId, cfg, topVer);
            }
            catch (Throwable e) {
                th = e;
            }

            if (th == null && !top.isEmpty()) {
                if (log.isDebugEnabled())
                    log.debug("Calculated service assignments: " + top);

                srvcsToDeploy.put(srvcId, top);
            }
            else {
                if (th != null) {
                    th = new IgniteException("Failed to determine suitable nodes to deploy service" +
                        ", srvcId=" + srvcId +
                        ", cfg=" + cfg, th);
                }

                Collection<Throwable> errors = depErrors.computeIfAbsent(srvcId, e -> new ArrayList<>());

                errors.add(th);

                srvcsToUndeploy.add(srvcId);
            }
        }

        expDeps.putAll(srvcsToDeploy);

        changeServices(srvcsToDeploy, srvcsToUndeploy);
    }

    /**
     * Deploys service with given ids {@param srvcsToDeploy} if a number of local instances less than its number in
     * given topology.
     *
     * Undeploys service with given ids {@param srvcsToDeploy}.
     *
     * @param srvcsToDeploy Services to deploy.
     * @param srvcsToUndeploy Services to undeploy.
     */
    private void changeServices(Map<IgniteUuid, Map<UUID, Integer>> srvcsToDeploy, Set<IgniteUuid> srvcsToUndeploy) {
        final GridServiceProcessor proc = ctx.service();

        final Map<IgniteUuid, ServiceInfo> srvcsDescs = ctx.service().services();

        final Map<IgniteUuid, Collection<ServiceContextImpl>> locSrvcs = ctx.service().localServices();

        final Map<IgniteUuid, Collection<Throwable>> errors = new HashMap<>();

        try {
            proc.exchange().exchangerBlockingSectionBegin();

            for (IgniteUuid srvcId : srvcsToUndeploy)
                proc.undeploy(srvcId);

            srvcsToDeploy.forEach((srvcId, top) -> {
                Integer expCnt = top.get(ctx.localNodeId());

                boolean needDeploy = false;

                if (expCnt != null && expCnt > 0) {
                    Collection<ServiceContextImpl> ctxs = locSrvcs.get(srvcId);

                    needDeploy = (ctxs == null) || (ctxs.size() != expCnt);
                }

                if (needDeploy) {
                    try {
                        ServiceInfo desc = srvcsDescs.get(srvcId);

                        proc.redeploy(srvcId, desc.configuration(), top);
                    }
                    catch (Error | RuntimeException t) {
                        Collection<Throwable> err = errors.computeIfAbsent(srvcId, e -> new ArrayList<>());

                        err.add(t);
                    }
                }
            });
        }
        finally {
            proc.exchange().exchangerBlockingSectionEnd();
        }

        createAndSendSingleMapMessage(exchId, errors);
    }

    /**
     * @param exchId Exchange id.
     * @param errors Deployment errors.
     */
    private void createAndSendSingleMapMessage(ServicesDeploymentExchangeId exchId,
        final Map<IgniteUuid, Collection<Throwable>> errors) {
        ServicesSingleMapMessage msg = createSingleMapMessage(exchId, errors);

        if (crdId == null) {
            if (log.isDebugEnabled()) {
                log.debug("Failed to resolve coordinator to perform services single map message" +
                    ", locId=" + ctx.localNodeId() +
                    ", msg=" + msg);
            }

            return;
        }

        try {
            ctx.io().sendToGridTopic(crdId, TOPIC_SERVICES, msg, SERVICE_POOL);

            if (log.isDebugEnabled())
                log.debug("Send services single assignments message, msg=" + msg);
        }
        catch (IgniteCheckedException e) {
            if (log.isDebugEnabled() && X.hasCause(e, ClusterTopologyCheckedException.class))
                log.debug("Topology changed while message send: " + e.getMessage());

            log.error("Failed to send message over communication spi, msg=" + msg, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onReceiveSingleMapMessage(UUID snd, ServicesSingleMapMessage msg) {
        assert exchId.equals(msg.exchangeId()) : "Wrong message's exchange id, msg=" + msg;

        initCrdFut.listen((IgniteInClosure<IgniteInternalFuture<?>>)fut -> {
            if (isCompleted())
                return;

            synchronized (initCrdMux) {
                if (remaining.remove(snd)) {
                    singleMapMsgs.put(snd, msg);

                    if (remaining.isEmpty())
                        onAllReceived();
                }
                else if (log.isDebugEnabled())
                    log.debug("Unexpected service single map received, msg=" + msg);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void onReceiveFullMapMessage(UUID snd, ServicesFullMapMessage msg) {
        assert exchId.equals(msg.exchangeId()) : "Wrong message's exchange id, msg=" + msg;

        initTaskFut.listen((IgniteInClosure<IgniteInternalFuture<?>>)fut -> {
            if (isCompleted())
                return;

            Throwable th = null;

            try {
                ctx.service().processFullMap(msg);
            }
            catch (Throwable t) {
                log.error("Failed to process services deployment full map, msg=" + msg, t);

                th = t;
            }
            finally {
                complete(th, false);
            }
        });
    }

    /**
     * Creates full assignments message and send it over discovery.
     */
    private void onAllReceived() {
        assert !isCompleted();

        final Map<IgniteUuid, Map<UUID, ServiceSingleDeploymentsResults>> singleResults = buildSingleDeploymentsResults();

        addDeploymentErrorsToDeploymentResults(singleResults);

        final Collection<ServiceFullDeploymentsResults> fullResults = buildFullDeploymentsResults(singleResults);

        ServicesFullMapMessage msg = new ServicesFullMapMessage(exchId, fullResults);

        try {
            ctx.discovery().sendCustomEvent(msg);
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to send message across the ring, msg=" + msg, e);
        }
    }

    /**
     * Reassigns service to nodes.
     *
     * @param cfg Service configuration.
     * @param topVer Topology version.
     * @throws IgniteCheckedException If failed.
     */
    public Map<UUID, Integer> reassign(IgniteUuid srvcId, ServiceConfiguration cfg,
        AffinityTopologyVersion topVer) throws IgniteCheckedException {
        try {
            if (lessThenLocalJoin(topVer))
                return Collections.emptyMap();

            return ctx.service().reassign(srvcId, cfg, topVer);
        }
        catch (Throwable e) {
            IgniteCheckedException ex = new IgniteCheckedException("Failed to calculate assignment for service, cfg=" + cfg, e);

            log.error(ex.getMessage(), ex);

            throw ex;
        }
    }

    /**
     * Processes single map messages to build single deployment results.
     *
     * @return Services single deployments results.
     */
    private Map<IgniteUuid, Map<UUID, ServiceSingleDeploymentsResults>> buildSingleDeploymentsResults() {
        final Map<IgniteUuid, Map<UUID, ServiceSingleDeploymentsResults>> results = new HashMap<>();

        singleMapMsgs.forEach((nodeId, msg) -> {
            msg.results().forEach((srvcId, res) -> {
                Map<UUID, ServiceSingleDeploymentsResults> depResults = results.computeIfAbsent(srvcId, r -> new HashMap<>());

                int cnt = res.count();

                if (cnt != 0) {
                    Map<UUID, Integer> expTop = expDeps.get(srvcId);

                    if (expTop != null) {
                        Integer expCnt = expTop.get(nodeId);

                        if (expCnt == null)
                            cnt = 0;
                        else
                            cnt = Math.min(cnt, expCnt);
                    }
                }

                ServiceSingleDeploymentsResults singleDepRes = new ServiceSingleDeploymentsResults(cnt);

                if (!res.errors().isEmpty())
                    singleDepRes.errors(res.errors());

                depResults.put(nodeId, singleDepRes);
            });
        });

        return results;
    }

    /**
     * @param results Services per node single deployments results.
     */
    private void addDeploymentErrorsToDeploymentResults(
        final Map<IgniteUuid, Map<UUID, ServiceSingleDeploymentsResults>> results) {
        depErrors.forEach((srvcId, errBytes) -> {
            Collection<byte[]> srvcErrors = new ArrayList<>(errBytes.size());

            for (Throwable th : errBytes) {
                try {
                    srvcErrors.add(U.marshal(ctx, th));
                }
                catch (IgniteCheckedException e) {
                    log.error("Failed to marshal reassignments error.", e);
                }
            }

            if (!srvcErrors.isEmpty()) {
                Map<UUID, ServiceSingleDeploymentsResults> depResults = results.computeIfAbsent(srvcId, r -> new HashMap<>());

                ServiceSingleDeploymentsResults singleDepRes = depResults.computeIfAbsent(
                    ctx.localNodeId(), r -> new ServiceSingleDeploymentsResults(0));

                if (singleDepRes.errors().isEmpty())
                    singleDepRes.errors(srvcErrors);
                else
                    singleDepRes.errors().addAll(srvcErrors);
            }
        });
    }

    /**
     * @param results Services per node single deployments results.
     * @return Services full deployments results.
     */
    private Collection<ServiceFullDeploymentsResults> buildFullDeploymentsResults(
        final Map<IgniteUuid, Map<UUID, ServiceSingleDeploymentsResults>> results) {
        final Map<IgniteUuid, ServiceInfo> srvcsDescs = ctx.service().services();

        final Collection<ServiceFullDeploymentsResults> fullResults = new ArrayList<>();

        results.forEach((srvcId, dep) -> {
            if (dep.values().stream().anyMatch(r -> !r.errors().isEmpty() && srvcsDescs.containsKey(srvcId))) {
                ServiceInfo srvcDesc = srvcsDescs.get(srvcId);

                ServiceDeploymentFailuresPolicy plc = srvcDesc.configuration().getPolicy();

                if (plc == ServiceDeploymentFailuresPolicy.CANCEL)
                    dep.values().forEach(r -> r.count(0));
            }

            ServiceFullDeploymentsResults res = new ServiceFullDeploymentsResults(srvcId, dep);

            fullResults.add(res);
        });

        return fullResults;
    }

    /**
     * @param exchId Exchange id.
     * @param errors Deployment errors.
     * @return Services single map message.
     */
    private ServicesSingleMapMessage createSingleMapMessage(ServicesDeploymentExchangeId exchId,
        Map<IgniteUuid, Collection<Throwable>> errors) {
        Map<IgniteUuid, ServiceSingleDeploymentsResults> results = new HashMap<>();

        ctx.service().localServices().forEach((id, ctxs) -> {
            ServiceSingleDeploymentsResults depRes = new ServiceSingleDeploymentsResults(ctxs.size());

            Collection<Throwable> err = errors.get(id);

            if (err != null && !err.isEmpty()) {
                Collection<byte[]> errorsBytes = new ArrayList<>();

                for (Throwable th : err) {
                    try {
                        byte[] arr = U.marshal(ctx, th);

                        errorsBytes.add(arr);
                    }
                    catch (IgniteCheckedException e) {
                        log.error("Failed to marshal deployment error, err=" + th, e);
                    }
                }

                depRes.errors(errorsBytes);
            }

            results.put(id, depRes);
        });

        return new ServicesSingleMapMessage(exchId, results);
    }

    /** {@inheritDoc} */
    @Override public void onNodeLeft(UUID nodeId) {
        if (isCompleted())
            return;

        initTaskFut.listen((IgniteInClosure<IgniteInternalFuture<?>>)fut -> {
            if (isCompleted())
                return;

            final boolean crdChanged = nodeId.equals(crdId);

            if (crdChanged) {
                ClusterNode crd = ctx.service().coordinator();

                if (crd != null) {
                    crdId = crd.id();

                    if (crd.isLocal())
                        initCoordinator(evtTopVer);
                }

                createAndSendSingleMapMessage(exchId, depErrors);
            }
            else if (ctx.localNodeId().equals(crdId)) {
                synchronized (initCrdMux) {
                    boolean rmvd = remaining.remove(nodeId);

                    if (rmvd && remaining.isEmpty()) {
                        singleMapMsgs.remove(nodeId);

                        onAllReceived();
                    }
                }
            }
        });
    }

    /**
     * @param topVer Topology version.
     * @return {@code true} if given topology version less then topology version of local join event, otherwise {@code
     * false}.
     */
    private boolean lessThenLocalJoin(AffinityTopologyVersion topVer) {
        return locJoinTopVer.compareTo(topVer) > 0;
    }

    /** {@inheritDoc} */
    @Override public ServicesDeploymentExchangeId exchangeId() {
        return exchId;
    }

    /** {@inheritDoc} */
    @Override public void complete(@Nullable Throwable err, boolean cancel) {
        onDone(null, err, cancel);

        if (!initTaskFut.isDone())
            initTaskFut.onDone();

        if (!initCrdFut.isDone())
            initCrdFut.onDone();
    }

    /** {@inheritDoc} */
    @Override public boolean isCompleted() {
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
        out.writeObject(exchId);
        out.writeObject(evt);
        out.writeObject(evtTopVer);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        exchId = (ServicesDeploymentExchangeId)in.readObject();
        evt = (DiscoveryEvent)in.readObject();
        evtTopVer = (AffinityTopologyVersion)in.readObject();
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