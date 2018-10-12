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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
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

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridTopic.TOPIC_SERVICES;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SERVICE_POOL;

/**
 * Services deployment exchange task.
 */
public class ServicesDeploymentExchangeTask implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Task's completion future. */
    private final GridFutureAdapter<?> completedFut = new GridFutureAdapter<>();

    /** Task's completion of initialization future. */
    private final GridFutureAdapter<?> initTaskFut = new GridFutureAdapter<>();

    /** Task's completion of remaining nodes ids initialization future. */
    private final GridFutureAdapter<?> initCrdFut = new GridFutureAdapter<>();

    /** Coordinator initialization actions mutex. */
    private final Object initCrdMux = new Object();

    /** Added in exchange queue flag. */
    private AtomicBoolean addedInQueue = new AtomicBoolean(false);

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

    /** Cause discovery event. */
    @GridToStringInclude
    private volatile DiscoveryEvent evt;

    /** Topology version. */
    @GridToStringInclude
    private volatile AffinityTopologyVersion evtTopVer;

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
    public ServicesDeploymentExchangeTask() {
    }

    /**
     * @param exchId Service deployment exchange id.
     */
    protected ServicesDeploymentExchangeTask(ServicesDeploymentExchangeId exchId) {
        this.exchId = exchId;
    }

    /**
     * Handles discovery event receiving.
     *
     * @param evt Discovery event.
     * @param topVer Topology version.
     */
    public void onEvent(DiscoveryEvent evt, AffinityTopologyVersion topVer) {
        assert evt.type() == EVT_NODE_JOINED || evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED
            || evt.type() == EVT_DISCOVERY_CUSTOM_EVT : "Unexpected event type, evt=" + evt;

        this.evt = evt;
        this.evtTopVer = topVer;
    }

    /**
     * Initializes exchange task.
     *
     * @param ctx Kernal context.
     * @throws IgniteCheckedException In case of an error.
     */
    public void init(@NotNull GridKernalContext ctx) throws IgniteCheckedException {
        if (isCompleted() || initTaskFut.isDone())
            return;

        this.ctx = ctx;
        this.log = ctx.log(getClass());
        this.locJoinTopVer = ctx.discovery().localJoin().joinTopologyVersion();

        final UUID evtNodeId = evt.eventNode().id();

        if (log.isDebugEnabled()) {
            log.debug("Started services exchange task init: [exchId=" + exchangeId() +
                ", locId=" + this.ctx.localNodeId() + ", evt=" + evt + ']');
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

            if (evt.type() == EVT_DISCOVERY_CUSTOM_EVT) {
                final DiscoveryCustomMessage customMsg = ((DiscoveryCustomEvent)evt).customMessage();

                assert customMsg != null : this;

                if (customMsg instanceof ChangeGlobalStateMessage)
                    onChangeGlobalStateMessage((ChangeGlobalStateMessage)customMsg, evtTopVer);
                else if (customMsg instanceof DynamicServicesChangeRequestBatchMessage)
                    onServiceChangeRequest(evtNodeId, (DynamicServicesChangeRequestBatchMessage)customMsg, evtTopVer);
                else if (!lessThenLocalJoin(evtTopVer)) {
                    if (customMsg instanceof DynamicCacheChangeBatch)
                        onCacheStateChangeRequest((DynamicCacheChangeBatch)customMsg);
                    else if (customMsg instanceof CacheAffinityChangeMessage)
                        initReassignment(ctx.service().affinityServices(), evtTopVer);
                    else
                        assert false : "Unexpected type of custom message, customMsg=" + customMsg;
                }
                else if (log.isDebugEnabled()) {
                    log.debug("Services exchange topology version less then local join topology. " +
                        "No actions required, waiting for services deployment full map : " +
                        "[locId=" + ctx.localNodeId() + ", exchId=" + exchId + ']');
                }
            }
            else {
                Set<IgniteUuid> toReassign = new HashSet<>();

                if (evt.type() == EVT_NODE_JOINED)
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
    private void onChangeGlobalStateMessage(ChangeGlobalStateMessage req, AffinityTopologyVersion topVer) {
        if (req.activate())
            initReassignment(ctx.service().registeredServicesIds(), topVer);
        else {
            ctx.service().onDeActivate(ctx);

            complete();
        }
    }

    /**
     * @param nodId Event node id.
     * @param batch Set of requests to change service.
     * @param topVer Topology version.
     */
    private void onServiceChangeRequest(UUID nodId, DynamicServicesChangeRequestBatchMessage batch,
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
                            Map<UUID, Integer> oldTop = null;

                            if (oldSrvcDesc != null) {
                                srvcId = oldSrvcDesc.serviceId();
                                oldTop = filterDeadNodes(oldSrvcDesc.topologySnapshot());
                            }

                            srvcTop = reassign(srvcId, cfg, topVer, oldTop);
                        }
                        catch (IgniteCheckedException e) {
                            err = e;

                            srvcTop = Collections.emptyMap();
                        }

                        ctx.service().putIfAbsentServiceInfo(srvcId, new ServiceInfo(nodId, srvcId, cfg));

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
        if (toReassign.isEmpty()) {
            complete();

            if (log.isDebugEnabled()) {
                log.debug("Services reassignments is not required, completed the task without exchange : " +
                    "[locId=" + ctx.localNodeId() + ", exchId=" + exchId + ']');
            }

            return;
        }

        for (IgniteUuid srvcId : toReassign) {
            ServiceInfo desc = ctx.service().serviceInfo(srvcId);

            assert desc != null;

            Map<UUID, Integer> top = null;

            Throwable err = null;

            try {
                top = reassign(srvcId, desc.configuration(), topVer, filterDeadNodes(desc.topologySnapshot()));
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
        assert crdId != null : "Coordinator should be defined at this point, locId=" + ctx.localNodeId();

        try {
            ServicesSingleMapMessage msg = createSingleMapMessage(exchId, errors);

            if (ctx.localNodeId().equals(crdId))
                onReceiveSingleMapMessage(ctx.localNodeId(), msg);
            else
                ctx.io().sendToGridTopic(crdId, TOPIC_SERVICES, msg, SERVICE_POOL);

            if (log.isDebugEnabled())
                log.debug("Send services single map message, msg=" + msg);
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to send services single map message to coordinator over communication spi.", e);
        }
    }

    /**
     * Handles received single node services map message.
     *
     * @param snd Sender node id.
     * @param msg Single services map message.
     */
    public void onReceiveSingleMapMessage(UUID snd, ServicesSingleMapMessage msg) {
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

    /**
     * Handles received full services map message.
     *
     * @param snd Sender node id.
     * @param msg Full services map message.
     */
    public void onReceiveFullMapMessage(UUID snd, ServicesFullMapMessage msg) {
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
     * @param oldTop Previous topology snapshot.
     * @throws IgniteCheckedException In case of an error.
     */
    private Map<UUID, Integer> reassign(IgniteUuid srvcId, ServiceConfiguration cfg,
        AffinityTopologyVersion topVer, Map<UUID, Integer> oldTop) throws IgniteCheckedException {
        try {
            if (lessThenLocalJoin(topVer))
                return Collections.emptyMap();

            Map<UUID, Integer> srvcTop = ctx.service().reassign(srvcId, cfg, topVer, oldTop);

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
     * Filters dead nodes from given service topology snapshot.
     *
     * @param top Service topology snapshot.
     * @return Filtered service topology snapshot.
     */
    private Map<UUID, Integer> filterDeadNodes(Map<UUID, Integer> top) {
        if (top == null || top.isEmpty())
            return top;

        Map<UUID, Integer> filtered = new HashMap<>();

        top.forEach((nodeId, cnt) -> {
            if (ctx.discovery().alive(nodeId))
                filtered.put(nodeId, cnt);
        });

        return filtered;
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

    /**
     * Handles situations when node leaves topology.
     *
     * @param nodeId Left node id.
     */
    public void onNodeLeft(UUID nodeId) {
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
                        initCoordinator(evtTopVer);

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

    /**
     * @return Cause discovery event.
     */
    public DiscoveryEvent event() {
        return evt;
    }

    /**
     * Returns cause of exchange topology version.
     *
     * @return Cause of exchange topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return evtTopVer;
    }

    /**
     * Returns services deployment exchange id of the task.
     *
     * @return Services deployment exchange id.
     */
    public ServicesDeploymentExchangeId exchangeId() {
        return exchId;
    }

    /**
     * Completes the task.
     */
    public void complete() {
        complete(null);
    }

    /**
     * @param err Error to complete with.
     */
    private void complete(Throwable err) {
        completedFut.onDone(err);

        if (!initTaskFut.isDone())
            initTaskFut.onDone();

        if (!initCrdFut.isDone())
            initCrdFut.onDone();
    }

    /**
     * Releases resources to reduce memory usages.
     */
    protected void clear() {
        singleMapMsgs.clear();
        expDeps.clear();
        depErrors.clear();
        remaining.clear();

        if (evt instanceof DiscoveryCustomEvent)
            ((DiscoveryCustomEvent)evt).customMessage(null);
    }

    /**
     * Returns if the task completed.
     *
     * @return {@code true} if the task completed, otherwise {@code false}.
     */
    protected boolean isCompleted() {
        return completedFut.isDone();
    }

    /**
     * Synchronously waits for completion of the task for up to the given timeout.
     *
     * @param timeout The maximum time to wait in milliseconds.
     * @throws IgniteCheckedException In case of an error.
     */
    protected void waitForComplete(long timeout) throws IgniteCheckedException {
        completedFut.get(timeout);
    }

    /**
     * Handles when this task is being added in exchange queue.
     * <p/>
     * Introduced to avoid overhead on calling of {@link Collection#contains(Object)}}.
     *
     * @return {@code true} if task is has not been added previously, otherwise {@code false}.
     */
    protected boolean onAdded() {
        return addedInQueue.compareAndSet(false, true);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(evt);
        out.writeObject(evtTopVer);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        evt = (DiscoveryEvent)in.readObject();
        evtTopVer = (AffinityTopologyVersion)in.readObject();
        exchId = ServicesDeploymentExchangeManager.exchangeId(evt, evtTopVer);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ServicesDeploymentExchangeTask fut1 = (ServicesDeploymentExchangeTask)o;

        return exchId.equals(fut1.exchId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return exchId.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServicesDeploymentExchangeTask.class, this,
            "locNodeId", (ctx != null ? ctx.localNodeId() : "unknown"),
            "crdId", crdId);
    }
}