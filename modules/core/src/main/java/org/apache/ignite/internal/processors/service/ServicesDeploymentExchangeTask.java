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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
 * Services deployment exchange task.
 */
public class ServicesDeploymentExchangeTask {
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

    /** Services deployment actions. */
    private volatile ServicesDeploymentActions depActions;

    /** Kernal context. */
    private GridKernalContext ctx;

    /** Logger. */
    private IgniteLogger log;

    /** Coordinator node id. */
    @GridToStringExclude
    private volatile UUID crdId;

    /**
     * @param ctx Kernal context.
     * @param exchId Service deployment exchange id.
     */
    protected ServicesDeploymentExchangeTask(GridKernalContext ctx, ServicesDeploymentExchangeId exchId) {
        this.exchId = exchId;
        this.ctx = ctx;
        this.log = ctx.log(getClass());
    }

    /**
     * Handles discovery event receiving.
     *
     * @param evt Discovery event.
     * @param topVer Topology version.
     * @param depActions Services deployment actions.
     */
    public void onEvent(@NotNull DiscoveryEvent evt, @NotNull AffinityTopologyVersion topVer,
        @Nullable ServicesDeploymentActions depActions) {
        assert evt.type() == EVT_NODE_JOINED || evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED
            || evt.type() == EVT_DISCOVERY_CUSTOM_EVT : "Unexpected event type, evt=" + evt;

        this.evt = evt;
        this.evtTopVer = topVer;
        this.depActions = depActions;
    }

    /**
     * Initializes exchange task.
     *
     * @throws IgniteCheckedException In case of an error.
     */
    public void init() throws IgniteCheckedException {
        if (isCompleted() || initTaskFut.isDone())
            return;

        if (log.isDebugEnabled()) {
            log.debug("Started services exchange task init: [exchId=" + exchangeId() +
                ", locId=" + this.ctx.localNodeId() + ", evt=" + evt + ']');
        }

        try {
            if (depActions != null && depActions.deactivate()) {
                ctx.service().onDeActivate(ctx);

                complete();

                return;
            }

            if (depActions == null) {
                if (evt.type() == EVT_DISCOVERY_CUSTOM_EVT) {
                    DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)evt).customMessage();

                    assert msg != null : "DiscoveryCustomEvent has been nullified concurrently.";

                    if (msg instanceof CacheAffinityChangeMessage) {
                        CacheAffinityChangeMessage msg0 = (CacheAffinityChangeMessage)msg;

                        Map<IgniteUuid, ServiceInfo> descs = ctx.service().startedServices();

                        if (!descs.isEmpty()) {
                            Map<Integer, Map<Integer, List<UUID>>> change = msg0.assignmentChange();

                            if (change != null) {
                                Set<String> names = new HashSet<>();

                                ctx.cache().cacheDescriptors().forEach((name, desc) -> {
                                    if (change.containsKey(desc.groupId()))
                                        names.add(name);
                                });

                                Map<IgniteUuid, ServiceInfo> toReassign = new HashMap<>();

                                descs.forEach((srvcId, desc) -> {
                                    if (names.contains(desc.cacheName()))
                                        toReassign.put(srvcId, desc);
                                });

                                if (!toReassign.isEmpty()) {
                                    depActions = new ServicesDeploymentActions();

                                    depActions.servicesToDeploy(toReassign);
                                }
                            }
                        }
                    }
                }
                else {
                    assert evt.type() == EVT_NODE_JOINED || evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED;

                    Map<IgniteUuid, ServiceInfo> toRedeploy = ctx.service().startedServices();

                    if (evt.type() == EVT_NODE_JOINED) {
                        if (evt.eventNode().isLocal())
                            toRedeploy.putAll(ctx.service().localJoinDeployServices());
                        else {
                            for (ServiceInfo desc : ctx.service().servicesReceivedFromJoin(evt.eventNode().id()))
                                toRedeploy.put(desc.serviceId(), desc);
                        }
                    }

                    if (!toRedeploy.isEmpty()) {
                        depActions = new ServicesDeploymentActions();

                        depActions.servicesToDeploy(toRedeploy);
                    }
                }
            }

            if (depActions != null) {
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

                processDeploymentActions(depActions);
            }
            else {
                complete();

                if (log.isDebugEnabled())
                    log.debug("No action required.");
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
     * @param depActions Services deployment actions.
     */
    private void processDeploymentActions(@NotNull ServicesDeploymentActions depActions) {
        if (depActions.deactivate()) {
            ctx.service().onDeActivate(ctx);

            complete();

            return;
        }

        final GridServiceProcessor proc = ctx.service();

        try {
            proc.updateStartedDescriptors(depActions);

            proc.exchange().exchangerBlockingSectionBegin();

            depActions.servicesToUndeploy().forEach((srvcId, desc) -> {
                proc.undeploy(srvcId);
            });

            depActions.servicesToDeploy().forEach((srvcId, desc) -> {
                try {
                    ServiceConfiguration cfg = desc.configuration();

                    Map<UUID, Integer> top = reassign(srvcId, cfg, evtTopVer, filterDeadNodes(desc.topologySnapshot()));

                    expDeps.put(srvcId, top);

                    Integer expCnt = top.getOrDefault(ctx.localNodeId(), 0);

                    boolean needDeploy = expCnt > 0 && proc.localInstancesCount(srvcId) != expCnt;

                    if (needDeploy)
                        proc.redeploy(srvcId, cfg, top);
                }
                catch (Error | RuntimeException | IgniteCheckedException err) {
                    depErrors.computeIfAbsent(srvcId, e -> new ArrayList<>()).add(err);
                }
            });
        }
        finally {
            proc.exchange().exchangerBlockingSectionEnd();
        }

        createAndSendSingleMapMessage(exchId, depErrors);
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
     * @param exchId Exchange id.
     * @param errors Deployment errors.
     */
    private void createAndSendSingleMapMessage(ServicesDeploymentExchangeId exchId,
        final Map<IgniteUuid, Collection<Throwable>> errors) {
        assert crdId != null : "Coordinator should be defined at this point, locId=" + ctx.localNodeId();

        try {
            Map<IgniteUuid, ServiceSingleDeploymentsResults> results = new HashMap<>();

            ctx.service().localInstancesCount().forEach((id, cnt) -> {
                ServiceSingleDeploymentsResults depRes = new ServiceSingleDeploymentsResults(cnt);

                Collection<Throwable> err = errors.get(id);

                if (err != null && !err.isEmpty())
                    fillDeploymentErrors(depRes, err);

                results.put(id, depRes);
            });

            errors.forEach((srvcId, err) -> {
                if (results.containsKey(srvcId))
                    return;

                ServiceSingleDeploymentsResults depRes = new ServiceSingleDeploymentsResults(0);

                if (err != null && !err.isEmpty())
                    fillDeploymentErrors(depRes, err);

                results.put(srvcId, depRes);
            });

            ServicesSingleMapMessage msg = new ServicesSingleMapMessage(exchId, results);

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

                //
//
//                final Collection<ServiceFullDeploymentsResults> results = msg.results();
//
//                final Map<IgniteUuid, HashMap<UUID, Integer>> fullTops = new HashMap<>();
//                final Map<IgniteUuid, Collection<byte[]>> fullErrors = new HashMap<>();
//
//                for (ServiceFullDeploymentsResults depRes : results) {
//                    final IgniteUuid srvcId = depRes.serviceId();
//                    final Map<UUID, ServiceSingleDeploymentsResults> deps = depRes.results();
//
//                    final HashMap<UUID, Integer> top = new HashMap<>();
//                    final Collection<byte[]> errors = new ArrayList<>();
//
//                    deps.forEach((nodeId, res) -> {
//                        int cnt = res.count();
//
//                        if (cnt > 0)
//                            top.put(nodeId, cnt);
//
//                        if (!res.errors().isEmpty())
//                            errors.addAll(res.errors());
//                    });
//
//                    if (!top.isEmpty())
//                        fullTops.put(srvcId, top);
//
//                    if (!errors.isEmpty())
//                        fullErrors.computeIfAbsent(srvcId, e -> new ArrayList<>()).addAll(errors);
//                }
//
//                ctx.service().processFullMap0(fullTops, fullErrors);
                //

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
            Map<UUID, Integer> top = ctx.service().reassign(srvcId, cfg, topVer, oldTop);

            if (top.isEmpty())
                throw new IgniteCheckedException("Failed to determine suitable nodes to deploy service.");

            if (log.isDebugEnabled())
                log.debug("Calculated service assignment : [srvcId=" + srvcId + ", srvcTop=" + top + ']');

            return top;
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
     * @return Services full deployments results.
     */
    private Collection<ServiceFullDeploymentsResults> buildFullDeploymentsResults(
        final Map<IgniteUuid, Map<UUID, ServiceSingleDeploymentsResults>> results) {

        final Collection<ServiceFullDeploymentsResults> fullResults = new ArrayList<>();

        final Map<IgniteUuid, ServiceInfo> descs = ctx.service().startedServices();

        results.forEach((srvcId, dep) -> {
            ServiceInfo desc = descs.get(srvcId);

            assert desc != null;

            ServiceConfiguration cfg = desc.configuration();

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
     * @param depRes Service single deployments results.
     * @param errors Deployment errors.
     */
    private void fillDeploymentErrors(@NotNull ServiceSingleDeploymentsResults depRes,
        @NotNull Collection<Throwable> errors) {
        Collection<byte[]> errorsBytes = new ArrayList<>();

        for (Throwable th : errors) {
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