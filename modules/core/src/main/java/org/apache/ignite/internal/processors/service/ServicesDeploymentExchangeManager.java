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

    /** Mutex. */
    private final Object newEvtMux = new Object();

    /** Services discovery messages listener. */
    private final DiscoveryEventListener discoLsnr = new ServiceDiscoveryListener();

    /** Services communication messages listener. */
    private final GridMessageListener commLsnr = new ServiceCommunicationListener();

    /** Services deploymentst tasks. */
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
    public ServicesDeploymentExchangeManager(GridKernalContext ctx) {
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
    public void startProcessing() {
        new IgniteThread(ctx.igniteInstanceName(), "services-deployment-exchange-worker", exchWorker).start();
    }

    /**
     * Stops processing of services deployments exchange tasks.
     */
    public void stopProcessing() {
        try {
            busyLock.block();

            ctx.event().removeDiscoveryEventListener(discoLsnr);

            ctx.io().removeMessageListener(commLsnr);

            U.cancel(exchWorker);

            synchronized (newEvtMux) {
                newEvtMux.notify();
            }

            U.join(exchWorker, log);

            exchWorker.tasksQueue.clear();

            pendingEvts.clear();

            tasks.values().forEach(t -> t.complete(null, true));

            tasks.clear();
        }
        catch (Exception e) {
            log.error("Error occurred during stopping exchange worker.");
        }
    }

    /**
     * Returns queue of deployments tasks.
     *
     * @return Queue of deployment tasks.
     */
    public ArrayDeque<ServicesDeploymentExchangeTask> tasks() {
        return exchWorker.tasksQueue;
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
    public void onLocalEvent(DiscoveryEvent evt, DiscoCache discoCache) {
        discoLsnr.onEvent(evt, discoCache);
    }

    /**
     * Invokes {@link GridWorker#blockingSectionBegin()} for service deployment exchange worker.
     * <p/>
     * Should be called from service deployment exchange worker thread.
     */
    public void exchangerBlockingSectionBegin() {
        assert exchWorker != null && Thread.currentThread() == exchWorker.runner();

        exchWorker.blockingSectionBegin();
    }

    /**
     * Invokes {@link GridWorker#blockingSectionEnd()} for service deployment exchange worker.
     * <p/>
     * Should be called from service deployment exchange worker thread.
     */
    public void exchangerBlockingSectionEnd() {
        assert exchWorker != null && Thread.currentThread() == exchWorker.runner();

        exchWorker.blockingSectionEnd();
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
            addEvent(evt, cache.version(), new ServicesDeploymentExchangeId(evt, cache.version()));
        else if (log.isDebugEnabled())
            log.debug("Ignore event, cluster is inactive, evt=" + evt);
    }

    /**
     * Addeds discovery event to exchange queue.
     *
     * @param evt Discovery event.
     * @param topVer Topology version.
     * @param exchId Exchange id.
     */
    public void addEvent(DiscoveryEvent evt, AffinityTopologyVersion topVer, ServicesDeploymentExchangeId exchId) {
        ServicesDeploymentExchangeTask task = tasks.computeIfAbsent(exchId, ServicesDeploymentExchangeFutureTask::new);

        synchronized (newEvtMux) {
            if (!exchWorker.tasksQueue.contains(task)) {
                task.event(evt, topVer);

                exchWorker.tasksQueue.add(task);
            }

            newEvtMux.notify();
        }
    }

    /**
     * Services discovery messages listener.
     */
    private class ServiceDiscoveryListener implements DiscoveryEventListener {
        /** {@inheritDoc} */
        @Override public void onEvent(final DiscoveryEvent evt, final DiscoCache discoCache) {
            if (!enterBusy())
                return;

            try {
                UUID snd = evt.eventNode().id();

                assert snd != null;

                switch (evt.type()) {
                    case EVT_DISCOVERY_CUSTOM_EVT:
                        DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)evt).customMessage();

                        if (msg instanceof ChangeGlobalStateFinishMessage) {
                            ChangeGlobalStateFinishMessage msg0 = (ChangeGlobalStateFinishMessage)msg;

                            if (msg0.clusterActive())
                                pendingEvts.forEach((t) -> addEvent(t.get1(), t.get2(),
                                    new ServicesDeploymentExchangeId(t.get1(), t.get2())));
                            else if (log.isDebugEnabled())
                                pendingEvts.forEach((t) -> log.debug("Ignore event, cluster is inactive: " + t.get1()));

                            pendingEvts.clear();
                        }
                        else if (msg instanceof ChangeGlobalStateMessage)
                            addEvent(evt, discoCache.version(), new ServicesDeploymentExchangeId(evt, discoCache.version()));

                        else if (msg instanceof DynamicServicesChangeRequestBatchMessage ||
                            msg instanceof DynamicCacheChangeBatch ||
                            msg instanceof CacheAffinityChangeMessage)
                            checkStateAndAddEvent(evt, discoCache);

                        else if (msg instanceof ServicesFullMapMessage) {
                            ServicesFullMapMessage msg0 = (ServicesFullMapMessage)msg;

                            if (log.isDebugEnabled()) {
                                log.debug("Received services full map message: [locId=" + ctx.localNodeId() +
                                    ", sender=" + snd +
                                    ", msg=" + msg0 + ']');
                            }

                            ServicesDeploymentExchangeId exchId = msg0.exchangeId();

                            if (tasks.containsKey(exchId)) { // In case of double delivering
                                ServicesDeploymentExchangeTask task = tasks.computeIfAbsent(exchId,
                                    ServicesDeploymentExchangeFutureTask::new);

                                task.onReceiveFullMapMessage(snd, msg0);
                            }
                        }

                        break;

                    case EVT_NODE_LEFT:
                    case EVT_NODE_FAILED:

                        tasks.values().forEach(t -> t.onNodeLeft(snd));

                    case EVT_NODE_JOINED:

                        checkStateAndAddEvent(evt, discoCache);

                        break;

                    default:
                        if (log.isDebugEnabled())
                            log.debug("Unexpected event was received, evt=" + evt);

                        break;
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

                    tasks.computeIfAbsent(msg0.exchangeId(), ServicesDeploymentExchangeFutureTask::new)
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

                    synchronized (newEvtMux) {
                        // Task shouldn't be removed from queue unless will be completed to avoid the possibility of losing
                        // event on newly joined node where the queue will be transferred.
                        task = tasksQueue.peek();

                        if (task == null) {
                            try {
                                blockingSectionBegin();

                                newEvtMux.wait();
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

                    try {
                        task.init(ctx);
                    }
                    catch (Exception e) {
                        log.error("Error occurred during init service exchange future.", e);

                        task.complete(e, false);

                        throw e;
                    }

                    final long dumpTimeout = 2 * ctx.config().getNetworkTimeout();

                    long nextDumpTime = 0;

                    while (true) {
                        try {
                            blockingSectionBegin();

                            task.waitForComplete(dumpTimeout);

                            taskPostProcessing(task);

                            break;
                        }
                        catch (IgniteFutureTimeoutCheckedException ignored) {
                            if (isCancelled)
                                return;

                            updateHeartbeat();

                            if (nextDumpTime <= U.currentTimeMillis()) {
                                log.warning("Failed to wait service deployment exchange or timeout had been reached" +
                                    ", timeout=" + dumpTimeout + ", task=" + task);

                                long limit = getLong(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT_LIMIT, 30 * 60_000);

                                limit = limit <= 0 ? 30 * 60_000 : limit;

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
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                if (!isCancelled)
                    err = e;
            }
            catch (Throwable t) {
                err = t;
            }
            finally {
                if (err == null && !isCancelled)
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

            tasks.remove(task.exchangeId());

            synchronized (newEvtMux) {
                tasksQueue.poll();
            }
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
