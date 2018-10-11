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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
    /** Default dump operation limit. */
    private static final long DFLT_DUMP_TIMEOUT_LIMIT = 30 * 60_000;

    /** Busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** Addition of new event mutex. */
    private final Object addEvtMux = new Object();

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

            synchronized (addEvtMux) {
                addEvtMux.notify();
            }

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
     * Returns queue of services deployment exchanges tasks.
     *
     * @return Queue of services deployment exchanges tasks.
     */
    protected ArrayDeque<ServicesDeploymentExchangeTask> tasks() {
        return exchWorker.tasksQueue;
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
     * Addeds discovery event to exchange queue.
     *
     * @param exchId Exchange id.
     * @param evt Discovery event.
     */
    protected void addEvent(ServicesDeploymentExchangeId exchId, DiscoveryEvent evt) {
        DiscoveryCustomMessage customMsg = null;

        if (evt.type() == EVT_DISCOVERY_CUSTOM_EVT) {
            DiscoveryCustomEvent customEvt = (DiscoveryCustomEvent)evt;

            customMsg = customEvt.customMessage();

            if (customEvt.customMessage() instanceof DynamicServicesChangeRequestBatchMessage)
                customEvt.customMessage(null);
        }

        addTask(exchId, customMsg);
    }

    /**
     * Addeds exchange task with given exchange id.
     *
     * @param exchId Exchange id.
     * @param customMsg Discovery custom message.
     */
    protected void addTask(@NotNull ServicesDeploymentExchangeId exchId, @Nullable DiscoveryCustomMessage customMsg) {
        ServicesDeploymentExchangeTask task = tasks.computeIfAbsent(exchId, t -> new ServicesDeploymentExchangeTask(exchId));

        if (task.onAdded()) {
            if (customMsg != null) // TODO
                task.customMessage(customMsg);

            synchronized (addEvtMux) {
                exchWorker.tasksQueue.add(task);

                addEvtMux.notify();
            }
        }
        else
            log.warning("Do not start service deployment exchange, exchId: " + exchId);
    }

    /**
     * Addeds discovery event to exchange queue with check if cluster is active or not.
     *
     * @param evt Discovery event.
     * @param cache Discovery cache.
     */
    private void checkStateAndAddEvent(DiscoveryEvent evt, DiscoCache cache) {
        if (cache.state().transition())
            pendingEvts.add(new IgniteBiTuple<>(evt, cache.version()));
        else if (cache.state().active())
            addEvent(new ServicesDeploymentExchangeId(evt, cache.version()), evt);
        else if (log.isDebugEnabled())
            log.debug("Ignore event, cluster is inactive, evt=" + evt);
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
                            pendingEvts.forEach((t) -> addEvent(new ServicesDeploymentExchangeId(t.get1(), t.get2()), t.get1()));
                        else if (log.isDebugEnabled())
                            pendingEvts.forEach((t) -> log.debug("Ignore event, cluster is inactive: " + t.get1()));

                        pendingEvts.clear();
                    }
                    else if (msg instanceof ChangeGlobalStateMessage)
                        addEvent(new ServicesDeploymentExchangeId(evt, discoCache.version()), evt);

                    else if (msg instanceof DynamicServicesChangeRequestBatchMessage ||
                        msg instanceof DynamicCacheChangeBatch ||
                        msg instanceof CacheAffinityChangeMessage)
                        checkStateAndAddEvent(evt, discoCache);

                    else if (msg instanceof ServicesFullMapMessage) {
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
                }
                else {
                    if (evtType == EVT_NODE_LEFT || evtType == EVT_NODE_FAILED)
                        tasks.values().forEach(t -> t.onNodeLeft(snd));

                    checkStateAndAddEvent(evt, discoCache);
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

                    tasks.computeIfAbsent(msg0.exchangeId(), t -> new ServicesDeploymentExchangeTask(msg0.exchangeId()))
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
        private final ArrayDeque<ServicesDeploymentExchangeTask> tasksQueue;

        /** {@inheritDoc} */
        private ServicesDeploymentExchangeWorker() {
            super(ctx.igniteInstanceName(), "services-deployment-exchanger",
                ServicesDeploymentExchangeManager.this.log, ctx.workersRegistry());

            this.tasksQueue = new ArrayDeque<>();
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            Throwable err = null;

            try {
                ServicesDeploymentExchangeTask task;

                while (!isCancelled()) {
                    onIdle();

                    synchronized (addEvtMux) {
                        // Task shouldn't be removed from queue unless will be completed to avoid the possibility of
                        // losing event on newly joined node where the queue will be transferred.
                        task = tasksQueue.peek();

                        if (task == null) {
                            blockingSectionBegin();

                            try {
                                addEvtMux.wait();
                            }
                            finally {
                                blockingSectionEnd();
                            }

                            continue;
                        }
                        else if (task.isCompleted()) {
                            tasksQueue.poll();

                            continue;
                        }
                    }

                    if (isCancelled())
                        Thread.currentThread().interrupt();

                    task.init(ctx);

                    final long dumpTimeout = 2 * ctx.config().getNetworkTimeout();

                    long nextDumpTime = dumpTimeout;

                    while (true) {
                        blockingSectionBegin();

                        try {
                            task.waitForComplete(nextDumpTime);

                            taskPostProcessing(task);

                            break;
                        }
                        catch (IgniteFutureTimeoutCheckedException ignored) {
                            if (isCancelled())
                                return;

                            if (nextDumpTime <= U.currentTimeMillis()) {
                                log.warning("Failed to wait service deployment exchange or timeout had been reached" +
                                    ", timeout=" + dumpTimeout + ", task=" + task);

                                long limit = getLong(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT_LIMIT, DFLT_DUMP_TIMEOUT_LIMIT);

                                limit = limit <= 0 ? DFLT_DUMP_TIMEOUT_LIMIT : limit;

                                long nextTimeout = U.currentTimeMillis() + dumpTimeout * 2;

                                nextDumpTime = nextTimeout <= limit ? nextTimeout : limit;
                            }

                            if (task.isCompleted()) {
                                taskPostProcessing(task);

                                break;
                            }
                        }
                        finally {
                            blockingSectionEnd();
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

            synchronized (addEvtMux) {
                tasksQueue.poll();
            }

            tasks.remove(task.exchangeId());
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
