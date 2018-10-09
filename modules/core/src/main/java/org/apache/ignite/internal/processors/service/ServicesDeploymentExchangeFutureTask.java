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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeRequest;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDeploymentFailuresPolicy;
import org.jetbrains.annotations.NotNull;
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
public class ServicesDeploymentExchangeFutureTask extends GridFutureAdapter
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

    /** Remaining nodes to received services single map message. */
    @GridToStringInclude
    private final Set<UUID> remaining = new HashSet<>();

    /** Exchange id. */
    @GridToStringInclude
    private ServicesDeploymentExchangeId exchId;

    /** Custom message. */
    @GridToStringInclude
    @Nullable private DiscoveryCustomMessage customMsg;

    /** Kernal context. */
    private GridKernalContext ctx;

    /** Logger. */
    private IgniteLogger log;

    /** Coordinator node id. */
    @GridToStringExclude
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
    protected ServicesDeploymentExchangeFutureTask(ServicesDeploymentExchangeId exchId) {
        this.exchId = exchId;
    }

    /** {@inheritDoc} */
    @Override public void customMessage(@NotNull DiscoveryCustomMessage customMsg) {
        assert exchId.requestId().equals(customMsg.id()) : "Wrong message's request id : " +
            "[expected=" + exchId.requestId() + ", actual=" + customMsg.id() + ']';

        this.customMsg = customMsg;
    }

    /** {@inheritDoc} */
    @Override public void init(@NotNull GridKernalContext ctx) throws IgniteCheckedException {
        if (isCompleted() || initTaskFut.isDone())
            return;

        this.ctx = ctx;
        this.log = ctx.log(getClass());
        this.locJoinTopVer = ctx.discovery().localJoin().joinTopologyVersion();

        final AffinityTopologyVersion evtTopVer = exchId.topologyVersion();
        final UUID evtNodeId = exchId.nodeId();
        final int evtType = exchId.eventType();

        assert ((evtType == EVT_NODE_JOINED || evtType == EVT_NODE_LEFT || evtType == EVT_NODE_FAILED) && customMsg == null) ||
            (evtType == EVT_DISCOVERY_CUSTOM_EVT && customMsg != null) : "evtType=" + evtType + "customMsg=" + customMsg;

        if (log.isDebugEnabled()) {
            log.debug("Started services exchange task init: [exchId=" + exchangeId() + ", locId=" + this.ctx.localNodeId() +
                ", evtType=" + U.gridEventName(evtType) + ", evtNodeId=" + evtNodeId + ", customMsg=" + customMsg + ']');
        }

        try {
            ClusterNode crd = U.oldest(ctx.discovery().aliveServerNodes(), null);

            if (crd == null) {
                complete();

                if (log.isDebugEnabled()) {
                    log.debug("Failed to resolve coordinator to process services map exchange: " +
                        "[locId=" + ctx.clientNode() + "client=" + ctx.clientNode() + ']');
                }

                return;
            }

            crdId = crd.id();

            if (crd.isLocal())
                initCoordinator(evtTopVer);

            if (evtType == EVT_DISCOVERY_CUSTOM_EVT) {
                if (customMsg instanceof ChangeGlobalStateMessage)
                    onChangeGlobalStateMessage((ChangeGlobalStateMessage)customMsg, evtTopVer);
                else if (customMsg instanceof DynamicServicesChangeRequestBatchMessage)
                    onServiceChangeRequest((DynamicServicesChangeRequestBatchMessage)customMsg, evtTopVer);
                else if (!lessThenLocalJoin(evtTopVer)) {
                    if (customMsg instanceof DynamicCacheChangeBatch)
                        onCacheStateChangeRequest((DynamicCacheChangeBatch)customMsg);
                    else if (customMsg instanceof CacheAffinityChangeMessage)
                        onCacheAffinityChangeMessage(evtTopVer);
                    else
                        assert false : "Unexpected type of custom message, customMsg=" + customMsg;
                }
            }
            else {
                Set<IgniteUuid> toReassign = new HashSet<>();

                if (evtType == EVT_NODE_JOINED)
                    toReassign.addAll(ctx.service().servicesReceivedFromJoin(evtNodeId));

                toReassign.addAll(ctx.service().deployedServicesIds());

                initReassignment(toReassign, evtTopVer);
            }

            if (log.isDebugEnabled()) {
                log.debug("Finished services exchange future init: [exchId=" + exchangeId() +
                    ", locId=" + this.ctx.localNodeId() + ']');
            }
        }
        catch (Exception e) {
            log.error("Error occurred while initializing exchange task, err=" + e.getMessage(), e);

            initTaskFut.onDone(e);

            complete(e);

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
                log.error("Error occurred while initializing remaining collection.", e);

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
     */
    private void onChangeGlobalStateMessage(ChangeGlobalStateMessage req,
        AffinityTopologyVersion topVer) {
        if (req.activate())
            initReassignment(ctx.service().registeredServicesIds(), topVer);
        else {
            ctx.service().onDeActivate(ctx);

            complete();
        }
    }

    /**
     * @param batch Set of requests to change service.
     * @param topVer Topology version.
     */
    private void onServiceChangeRequest(DynamicServicesChangeRequestBatchMessage batch,
        AffinityTopologyVersion topVer) {
        final Map<IgniteUuid, Map<UUID, Integer>> srvcsToDeploy = new HashMap<>();
        final Set<IgniteUuid> srvcsToUndeploy = new HashSet<>();

        for (DynamicServiceChangeRequest req : batch.requests()) {
            IgniteUuid reqSrvcId = req.serviceId();

            if (req.undeploy())
                srvcsToUndeploy.add(reqSrvcId);
            else if (req.deploy()) {
                IgniteUuid srvcId = reqSrvcId;
                ServiceConfiguration cfg = req.configuration();

                Exception err = null;

                ServiceInfo oldSrvcDesc = ctx.service().serviceInfo(srvcId);

                if (oldSrvcDesc != null) { // In case of a collision of IgniteUuid.randomUuid() (almost impossible case)
                    err = new IgniteCheckedException("Failed to deploy service. Service with generated id already exists : [" +
                        "srvcId" + reqSrvcId + ", srvcTop=" + oldSrvcDesc.topologySnapshot() + ']');
                }
                else {
                    oldSrvcDesc = ctx.service().serviceInfo(cfg.getName());

                    if (oldSrvcDesc != null && !oldSrvcDesc.configuration().equalsIgnoreNodeFilter(cfg)) {
                        err = new IgniteCheckedException("Failed to deploy service (service already exists with " +
                            "different configuration) : [deployed=" + oldSrvcDesc.configuration() + ", new=" + cfg + ']');
                    }
                    else {
                        Map<UUID, Integer> srvcTop;

                        try {
                            if (oldSrvcDesc != null)
                                srvcId = oldSrvcDesc.serviceId();

                            srvcTop = reassign(srvcId, cfg, topVer);
                        }
                        catch (IgniteCheckedException e) {
                            err = e;

                            srvcTop = Collections.emptyMap();
                        }

                        ctx.service().putIfAbsentServiceInfo(srvcId, new ServiceInfo(exchId.nodeId(), srvcId, cfg));

                        if (srvcTop != null && !srvcTop.isEmpty())
                            srvcsToDeploy.put(reqSrvcId, srvcTop);
                    }
                }

                if (err != null)
                    depErrors.computeIfAbsent(reqSrvcId, e -> new ArrayList<>()).add(err);
            }
        }

        if (!lessThenLocalJoin(topVer))
            changeServices(srvcsToDeploy, srvcsToUndeploy);
    }

    /**
     * @param topVer Topology version.
     */
    private void onCacheAffinityChangeMessage(AffinityTopologyVersion topVer) {
        Set<IgniteUuid> toReassign = ctx.service().affinityServices();

        if (toReassign.isEmpty()) {
            complete();

            return;
        }

        initReassignment(toReassign, topVer);
    }

    /**
     * @param req Cache state change request.
     */
    private void onCacheStateChangeRequest(DynamicCacheChangeBatch req) {
        Set<IgniteUuid> srvcsToUndeploy = new HashSet<>();

        for (DynamicCacheChangeRequest chReq : req.requests()) {
            if (chReq.stop()) {
                IgniteUuid srvcId = ctx.service().affinityService(chReq.cacheName());

                if (srvcId != null)
                    srvcsToUndeploy.add(srvcId);
            }
        }

        if (srvcsToUndeploy.isEmpty()) {
            complete();

            return;
        }

        changeServices(Collections.emptyMap(), srvcsToUndeploy);
    }

    /**
     * @param toReassign Services to reassign.
     * @param topVer Topology version.
     */
    private void initReassignment(Set<IgniteUuid> toReassign, AffinityTopologyVersion topVer) {
        final Map<IgniteUuid, Map<UUID, Integer>> srvcsToDeploy = new HashMap<>();

        for (IgniteUuid srvcId : toReassign) {
            ServiceConfiguration cfg = ctx.service().serviceConfiguration(srvcId);

            assert cfg != null;

            Map<UUID, Integer> top = null;

            Throwable err = null;

            try {
                top = reassign(srvcId, cfg, topVer);
            }
            catch (Throwable e) {
                err = e;
            }

            if (err != null)
                depErrors.computeIfAbsent(srvcId, e -> new ArrayList<>()).add(err);
            else if (top != null && !top.isEmpty())
                srvcsToDeploy.put(srvcId, top);
        }

        expDeps.putAll(srvcsToDeploy);

        changeServices(srvcsToDeploy, Collections.emptySet());
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

        final Map<IgniteUuid, Collection<Throwable>> errors = new HashMap<>();

        try {
            proc.exchange().exchangerBlockingSectionBegin();

            for (IgniteUuid srvcId : srvcsToUndeploy)
                proc.undeploy(srvcId);

            srvcsToDeploy.forEach((srvcId, top) -> {
                Integer expCnt = top.getOrDefault(ctx.localNodeId(), 0);

                boolean needDeploy = expCnt > 0 && proc.localInstancesCount(srvcId) != expCnt;

                if (needDeploy) {
                    try {
                        ServiceConfiguration cfg = ctx.service().serviceConfiguration(srvcId);

                        assert cfg != null;

                        proc.redeploy(srvcId, cfg, top);
                    }
                    catch (Error | RuntimeException err) {
                        errors.computeIfAbsent(srvcId, e -> new ArrayList<>()).add(err);
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
        assert crdId != null : "Failed to resolve coordinator to perform services single map message," +
            " locId=" + ctx.localNodeId();

        try {
            ServicesSingleMapMessage msg = createSingleMapMessage(exchId, errors);

            ctx.io().sendToGridTopic(crdId, TOPIC_SERVICES, msg, SERVICE_POOL);

            if (log.isDebugEnabled())
                log.debug("Send services single map message, msg=" + msg);
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to send services single map message to coordinator over communication spi.", e);
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

            ctx.closure().runLocalSafe(() -> {
                Throwable th = null;

                try {
                    ctx.service().processFullMap(msg);
                }
                catch (Throwable t) {
                    log.error("Failed to process services deployment full map, msg=" + msg, t);

                    th = t;
                }
                finally {
                    complete(th);
                }
            });
        });
    }

    /**
     * Creates services full map message and send it over discovery.
     */
    private void onAllReceived() {
        assert !isCompleted();

        try {
            final Map<IgniteUuid, Map<UUID, ServiceSingleDeploymentsResults>> singleResults = buildSingleDeploymentsResults();

            addDeploymentErrorsToDeploymentResults(singleResults);

            final Collection<ServiceFullDeploymentsResults> fullResults = buildFullDeploymentsResults(singleResults);

            ServicesFullMapMessage msg = new ServicesFullMapMessage(exchId, fullResults);

            ctx.discovery().sendCustomEvent(msg);
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to send services full map message across the ring.", e);
        }
    }

    /**
     * Reassigns service to nodes.
     *
     * @param cfg Service configuration.
     * @param topVer Topology version.
     * @throws IgniteCheckedException In case of an error.
     */
    private Map<UUID, Integer> reassign(IgniteUuid srvcId, ServiceConfiguration cfg,
        AffinityTopologyVersion topVer) throws IgniteCheckedException {
        try {
            if (lessThenLocalJoin(topVer))
                return Collections.emptyMap();

            Map<UUID, Integer> srvcTop = ctx.service().reassign(srvcId, cfg, topVer);

            if (srvcTop.isEmpty())
                throw new IgniteCheckedException("Failed to determine suitable nodes to deploy service.");

            if (log.isDebugEnabled())
                log.debug("Calculated service assignment : [srvcId=" + srvcId + ", srvcTop=" + srvcTop + ']');

            return srvcTop;
        }
        catch (Throwable e) {
            throw new IgniteCheckedException("Failed to calculate assignments for service, cfg=" + cfg, e);
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
                    log.error("Failed to marshal deployments error.", e);
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

        final Collection<ServiceFullDeploymentsResults> fullResults = new ArrayList<>();

        results.forEach((srvcId, dep) -> {
            ServiceConfiguration cfg = ctx.service().serviceConfiguration(srvcId);

            if (dep.values().stream().anyMatch(r -> !r.errors().isEmpty() && cfg != null)) {
                ServiceDeploymentFailuresPolicy plc = cfg.getPolicy();

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
        assert !isCompleted();

        Map<IgniteUuid, ServiceSingleDeploymentsResults> results = new HashMap<>();

        ctx.service().localInstancesCount().forEach((id, cnt) -> {
            ServiceSingleDeploymentsResults depRes = new ServiceSingleDeploymentsResults(cnt);

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
                ClusterNode crd = U.oldest(ctx.discovery().aliveServerNodes(), null);

                if (crd != null) {
                    crdId = crd.id();

                    if (crd.isLocal())
                        initCoordinator(exchId.topologyVersion());

                    createAndSendSingleMapMessage(exchId, depErrors);
                }
                else
                    complete();
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
        assert locJoinTopVer != null;

        return locJoinTopVer.compareTo(topVer) > 0;
    }

    /** {@inheritDoc} */
    @Override public ServicesDeploymentExchangeId exchangeId() {
        return exchId;
    }

    /** {@inheritDoc} */
    @Override public int eventType() {
        return exchId.eventType();
    }

    /** {@inheritDoc} */
    @Override public UUID nodeId() {
        return exchId.nodeId();
    }

    /** {@inheritDoc} */
    @Override public DiscoveryCustomMessage customMessage() {
        return customMsg;
    }

    /** {@inheritDoc} */
    @Override public void complete() {
        complete(null);
    }

    /**
     * @param err Error to complete with.
     */
    private void complete(Throwable err) {
        onDone(null, err, false);

        if (!initTaskFut.isDone())
            initTaskFut.onDone();

        if (!initCrdFut.isDone())
            initCrdFut.onDone();

        singleMapMsgs.clear();
        expDeps.clear();
        depErrors.clear();
        remaining.clear();
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
    @Override public AffinityTopologyVersion topologyVersion() {
        return exchId.topologyVersion();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(exchId);
        out.writeObject(customMsg);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        exchId = (ServicesDeploymentExchangeId)in.readObject();
        customMsg = (DiscoveryCustomMessage)in.readObject();
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
        return S.toString(ServicesDeploymentExchangeFutureTask.class, this,
            "locNodeId", (ctx != null ? ctx.localNodeId() : "unknown"),
            "crdId", crdId);
    }
}