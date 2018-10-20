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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateFinishMessage;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT_LIMIT;
import static org.apache.ignite.IgniteSystemProperties.getLong;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;
import static org.apache.ignite.internal.GridTopic.TOPIC_SERVICES;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;

/**
 * Services deployment exchange manager.
 */
public class ServicesDeploymentExchangeManager {
    /** Busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** Services discovery messages listener. */
    private final DiscoveryEventListener discoLsnr = new ServiceDiscoveryListener();

    /** Services communication messages listener. */
    private final GridMessageListener commLsnr = new ServiceCommunicationListener();

    /** Services deployments tasks. */
    private final Map<ServicesDeploymentExchangeId, ServicesDeploymentExchangeTask> tasks = new ConcurrentHashMap<>();

    /** Discovery events received while cluster state transition was in progress. */
    private final List<IgniteBiTuple<DiscoveryEvent, AffinityTopologyVersion>> pendingEvts = new ArrayList<>();

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Exchange worker. */
    private final ServicesDeploymentExchangeWorker exchWorker;

    /** Default dump operation limit. */
    private final long dfltDumpTimeoutLimit;

    /** Topology version of latest deployment task's event. */
    private final AtomicReference<AffinityTopologyVersion> readyTopVer =
        new AtomicReference<>(AffinityTopologyVersion.NONE);

    /**
     * @param ctx Grid kernal context.
     */
    protected ServicesDeploymentExchangeManager(@NotNull GridKernalContext ctx) {
        this.ctx = ctx;
        this.log = ctx.log(getClass());

        ctx.event().addDiscoveryEventListener(discoLsnr,
            EVT_NODE_JOINED, EVT_NODE_LEFT, EVT_NODE_FAILED, EVT_DISCOVERY_CUSTOM_EVT);

        ctx.io().addMessageListener(TOPIC_SERVICES, commLsnr);

        this.exchWorker = new ServicesDeploymentExchangeWorker();

        long limit = getLong(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT_LIMIT, 0);

        dfltDumpTimeoutLimit = limit <= 0 ? 30 * 60_000 : limit;
    }

    /**
     * Starts processing of services deployments exchange tasks.
     */
    protected void startProcessing() {
        new IgniteThread(ctx.igniteInstanceName(), "services-deployment-exchange-worker", exchWorker).start();
    }

    /**
     * Stops processing of services deployments exchange tasks.
     */
    protected void stopProcessing() {
        try {
            busyLock.block(); // Will not release it.

            ctx.event().removeDiscoveryEventListener(discoLsnr);

            ctx.io().removeMessageListener(commLsnr);

            U.cancel(exchWorker);

            U.join(exchWorker, log);

            exchWorker.tasksQueue.clear();

            pendingEvts.clear();

            tasks.values().forEach(ServicesDeploymentExchangeTask::complete);

            tasks.clear();
        }
        catch (Exception e) {
            log.error("Error occurred during stopping exchange worker.", e);
        }
    }

    /**
     * @return Ready topology version.
     */
    public AffinityTopologyVersion readyTopologyVersion() {
        return readyTopVer.get();
    }

    /**
     * Special handler for local discovery events for which the regular events are not generated, e.g. local join and
     * client reconnect events.
     *
     * @param evt Discovery event.
     * @param discoCache Discovery cache.
     */
    protected void onLocalEvent(DiscoveryEvent evt, DiscoCache discoCache) {
        discoLsnr.onEvent(evt, discoCache);
    }

    /**
     * Invokes {@link GridWorker#blockingSectionBegin()} for service deployment exchange worker.
     * <p/>
     * Should be called from service deployment exchange worker thread.
     */
    protected void exchangerBlockingSectionBegin() {
        assert exchWorker != null && Thread.currentThread() == exchWorker.runner();

        exchWorker.blockingSectionBegin();
    }

    /**
     * Invokes {@link GridWorker#blockingSectionEnd()} for service deployment exchange worker.
     * <p/>
     * Should be called from service deployment exchange worker thread.
     */
    protected void exchangerBlockingSectionEnd() {
        assert exchWorker != null && Thread.currentThread() == exchWorker.runner();

        exchWorker.blockingSectionEnd();
    }

    /**
     * Addeds exchange task with given exchange id.
     *
     * @param evt Discovery event.
     * @param topVer Topology version.
     * @param depActions Services deployment actions.
     */
    private void addTask(@NotNull DiscoveryEvent evt, @NotNull AffinityTopologyVersion topVer,
        @Nullable ServicesDeploymentActions depActions) {
        final ServicesDeploymentExchangeId exchId = exchangeId(evt, topVer);

        ServicesDeploymentExchangeTask task = tasks.computeIfAbsent(exchId, t -> new ServicesDeploymentExchangeTask(ctx, exchId));

        if (task.onAdded()) {
            assert task.event() == null && task.topologyVersion() == null;

            task.onEvent(evt, topVer, depActions);

            exchWorker.tasksQueue.add(task);
        }
        else
            log.warning("Do not start service deployment exchange for event: " + evt);
    }

    /**
     * Creates service exchange id.
     *
     * @param evt Discovery event.
     * @param topVer Topology version.
     * @return Services deployment exchange id.
     */
    public static ServicesDeploymentExchangeId exchangeId(@NotNull DiscoveryEvent evt,
        @NotNull AffinityTopologyVersion topVer) {
        if (evt instanceof DiscoveryCustomEvent)
            return new ServicesDeploymentExchangeId(((DiscoveryCustomEvent)evt).customMessage().id());
        else
            return new ServicesDeploymentExchangeId(topVer);
    }

    /**
     * Clones some instances of {@link DiscoveryCustomEvent} because their custom messages may be nullifyed earlier by
     * other subsystems then they will be processed by services exchange worker.
     *
     * @param evt Discovery event.
     * @return Discovery event to process.
     */
    private DiscoveryCustomEvent copyIfNeeded(@NotNull DiscoveryCustomEvent evt) {
        DiscoveryCustomMessage msg = evt.customMessage();

        assert msg != null;

        if (msg instanceof DynamicServicesChangeRequestBatchMessage)
            return evt;

        DiscoveryCustomEvent cp = new DiscoveryCustomEvent();

        cp.customMessage(evt.customMessage());
        cp.eventNode(evt.eventNode());
        cp.affinityTopologyVersion(evt.affinityTopologyVersion());

        return cp;
    }

    /**
     * Services discovery messages listener.
     */
    private class ServiceDiscoveryListener implements DiscoveryEventListener {
        /** {@inheritDoc} */
        @Override public void onEvent(final DiscoveryEvent evt, final DiscoCache discoCache) {
            if (!enterBusy())
                return;

            final UUID snd = evt.eventNode().id();
            final int evtType = evt.type();

            assert snd != null : "Event's node id shouldn't be null.";
            assert evtType == EVT_NODE_JOINED || evtType == EVT_NODE_LEFT || evtType == EVT_NODE_FAILED
                || evtType == EVT_DISCOVERY_CUSTOM_EVT : "Unexpected event was received, evt=" + evt;

            try {
                if (evtType == EVT_DISCOVERY_CUSTOM_EVT) {
                    DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)evt).customMessage();

                    if (msg instanceof ChangeGlobalStateFinishMessage) {
                        ChangeGlobalStateFinishMessage msg0 = (ChangeGlobalStateFinishMessage)msg;

                        if (msg0.clusterActive())
                            pendingEvts.forEach((t) -> addTask(t.get1(), t.get2(), null));
                        else if (log.isDebugEnabled())
                            pendingEvts.forEach((t) -> log.debug("Ignore event, cluster is inactive: " + t.get1()));

                        pendingEvts.clear();
                    }
                    else {
                        if (msg instanceof ServicesFullMapMessage) {
                            ServicesFullMapMessage msg0 = (ServicesFullMapMessage)msg;

                            if (log.isDebugEnabled()) {
                                log.debug("Received services full map message: [locId=" + ctx.localNodeId() +
                                    ", snd=" + snd + ", msg=" + msg0 + ']');
                            }

                            ServicesDeploymentExchangeId exchId = msg0.exchangeId();

                            assert exchId != null;

                            ServicesDeploymentExchangeTask task = tasks.get(exchId);

                            if (task != null) // May be null in case of double delivering
                                task.onReceiveFullMapMessage(snd, msg0);
                        }
                        else if (msg instanceof CacheAffinityChangeMessage)
                            addTask(copyIfNeeded((DiscoveryCustomEvent)evt), discoCache.version(), null);
                        else {
                            ServicesDeploymentActions depActions = null;

                            if (msg instanceof ChangeGlobalStateMessage)
                                depActions = ((ChangeGlobalStateMessage)msg).servicesDeploymentsActions();
                            else if (msg instanceof DynamicServicesChangeRequestBatchMessage)
                                depActions = ((DynamicServicesChangeRequestBatchMessage)msg).servicesDeploymentsActions();
                            else if (msg instanceof DynamicCacheChangeBatch)
                                depActions = ((DynamicCacheChangeBatch)msg).servicesDeploymentsActions();

                            if (depActions != null)
                                addTask(copyIfNeeded((DiscoveryCustomEvent)evt), discoCache.version(), depActions);
                        }
                    }
                }
                else {
                    if (evtType == EVT_NODE_LEFT || evtType == EVT_NODE_FAILED)
                        tasks.values().forEach(t -> t.onNodeLeft(snd));

                    if (discoCache.state().transition())
                        pendingEvts.add(new IgniteBiTuple<>(evt, discoCache.version()));
                    else if (discoCache.state().active())
                        addTask(evt, discoCache.version(), null);
                    else if (log.isDebugEnabled())
                        log.debug("Ignore event, cluster is inactive, evt=" + evt);
                }
            }
            finally {
                leaveBusy();
            }
        }
    }

    /**
     * Services messages communication listener.
     */
    private class ServiceCommunicationListener implements GridMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
            if (!enterBusy())
                return;

            try {
                if (msg instanceof ServicesSingleMapMessage) {
                    ServicesSingleMapMessage msg0 = (ServicesSingleMapMessage)msg;

                    if (log.isDebugEnabled()) {
                        log.debug("Received services single map message, locId=" + ctx.localNodeId() +
                            ", msg=" + msg0 + ']');
                    }

                    tasks.computeIfAbsent(msg0.exchangeId(), t -> new ServicesDeploymentExchangeTask(ctx, msg0.exchangeId()))
                        .onReceiveSingleMapMessage(nodeId, msg0);
                }
            }
            finally {
                leaveBusy();
            }
        }
    }

    /**
     * Services deployment exchange worker.
     */
    private class ServicesDeploymentExchangeWorker extends GridWorker {
        /** Queue to process. */
        private final LinkedBlockingDeque<ServicesDeploymentExchangeTask> tasksQueue = new LinkedBlockingDeque<>();

        /** {@inheritDoc} */
        private ServicesDeploymentExchangeWorker() {
            super(ctx.igniteInstanceName(), "services-deployment-exchanger",
                ServicesDeploymentExchangeManager.this.log, ctx.workersRegistry());
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            Throwable err = null;

            try {
                ServicesDeploymentExchangeTask task;

                while (!isCancelled()) {
                    onIdle();

                    task = tasksQueue.take();

                    if (isCancelled())
                        Thread.currentThread().interrupt();

                    task.init();

                    final long dumpTimeout = 2 * ctx.config().getNetworkTimeout();

                    long dumpCnt = 0;
                    long nextDumpTime = 0;

                    while (true) {
                        try {
                            blockingSectionBegin();

                            try {
                                task.waitForComplete(dumpTimeout);
                            }
                            finally {
                                blockingSectionEnd();
                            }

                            taskPostProcessing(task);

                            break;
                        }
                        catch (IgniteFutureTimeoutCheckedException ignored) {
                            if (isCancelled())
                                return;

                            if (nextDumpTime <= U.currentTimeMillis()) {
                                log.warning("Failed to wait service deployment exchange or timeout had been reached" +
                                    ", timeout=" + dumpTimeout + ", task=" + task);

                                long nextTimeout = dumpTimeout * (2 + dumpCnt++);

                                nextDumpTime = U.currentTimeMillis() + Math.min(nextTimeout, dfltDumpTimeoutLimit);
                            }
                        }
                    }
                }
            }
            catch (InterruptedException | IgniteInterruptedCheckedException e) {
                Thread.currentThread().interrupt();

                if (!isCancelled())
                    err = e;
            }
            catch (Throwable t) {
                err = t;
            }
            finally {
                if (err == null && !isCancelled())
                    err = new IllegalStateException("Worker " + name() + " is terminated unexpectedly.");

                if (err instanceof OutOfMemoryError)
                    ctx.failure().process(new FailureContext(CRITICAL_ERROR, err));
                else if (err != null)
                    ctx.failure().process(new FailureContext(SYSTEM_WORKER_TERMINATION, err));
            }
        }

        /**
         * Does additional actions after task's completion.
         */
        private void taskPostProcessing(ServicesDeploymentExchangeTask task) {
            AffinityTopologyVersion readyVer = readyTopVer.get();

            readyTopVer.compareAndSet(readyVer, task.topologyVersion());

            tasks.remove(task.exchangeId()).clear();
        }
    }

    /**
     * Enters busy state.
     *
     * @return {@code true} if entered to busy state.
     */
    private boolean enterBusy() {
        return busyLock.enterBusy();
    }

    /**
     * Leaves busy state.
     */
    private void leaveBusy() {
        busyLock.leaveBusy();
    }
}
