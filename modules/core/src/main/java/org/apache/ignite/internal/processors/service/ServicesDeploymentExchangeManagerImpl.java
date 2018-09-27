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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.GridKernalContext;
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
public class ServicesDeploymentExchangeManagerImpl implements ServicesDeploymentExchangeManager {
    /** Busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** Mutex. */
    private final Object mux = new Object();

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
    public ServicesDeploymentExchangeManagerImpl(GridKernalContext ctx) {
        this.ctx = ctx;
        this.log = ctx.log(getClass());

        ctx.event().addDiscoveryEventListener(discoLsnr,
            EVT_NODE_JOINED, EVT_NODE_LEFT, EVT_NODE_FAILED, EVT_DISCOVERY_CUSTOM_EVT);

        ctx.io().addMessageListener(TOPIC_SERVICES, commLsnr);

        this.exchWorker = new ServicesDeploymentExchangeWorker();
    }

    /** {@inheritDoc} */
    @Override public void startProcessing() {
        new IgniteThread(ctx.igniteInstanceName(), "services-deployment-exchange-worker", exchWorker).start();
    }

    /** {@inheritDoc} */
    @Override public void stopProcessing() {
        try {
            busyLock.block();

            ctx.event().removeDiscoveryEventListener(discoLsnr);

            ctx.io().removeMessageListener(commLsnr);

            U.cancel(exchWorker);

            U.join(exchWorker, log);

            synchronized (mux) {
                mux.notifyAll();
            }

            pendingEvts.clear();

            tasks.values().forEach(t -> t.complete(null, true));

            tasks.clear();

            exchWorker.tasksQueue.clear();
        }
        catch (Exception e) {
            log.error("Error occurred during stopping exchange worker.");
        }
    }

    /** {@inheritDoc} */
    @Override public void addTask(ServicesDeploymentExchangeTask task) {
        addEvent(task.event(), task.topologyVersion());
    }

    /** {@inheritDoc} */
    @Override public ArrayDeque<ServicesDeploymentExchangeTask> tasks() {
        return exchWorker.tasksQueue;
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion readyTopologyVersion() {
        return readyTopVer.get();
    }

    /** {@inheritDoc} */
    @Override public void onLocalEvent(DiscoveryEvent evt, DiscoCache discoCache) {
        discoLsnr.onEvent(evt, discoCache);
    }

    /** {@inheritDoc} */
    @Override public void exchangerBlockingSectionBegin() {
        assert exchWorker != null && Thread.currentThread() == exchWorker.runner();

        exchWorker.blockingSectionBegin();
    }

    /** {@inheritDoc} */
    @Override public void exchangerBlockingSectionEnd() {
        assert exchWorker != null && Thread.currentThread() == exchWorker.runner();

        exchWorker.blockingSectionEnd();
    }

    /**
     * Handles discovery event with check if cluster is active or not.
     *
     * @param evt Discovery event.
     * @param cache Discovery cache.
     */
    private void processEvent(DiscoveryEvent evt, DiscoCache cache) {
        if (cache.state().transition())
            pendingEvts.add(new IgniteBiTuple<>(evt, cache.version()));
        else if (cache.state().active())
            addEvent(evt, cache.version());
        else if (log.isDebugEnabled())
            log.debug("Ignore event, cluster is inactive, evt=" + evt);
    }

    /**
     * Addeds discovery event to exchange queue.
     *
     * @param evt Discovery event.
     * @param topVer Topology version.
     */
    private void addEvent(DiscoveryEvent evt, AffinityTopologyVersion topVer) {
        ServicesDeploymentExchangeId exchId = new ServicesDeploymentExchangeId(evt, topVer);

        ServicesDeploymentExchangeTask task = exchangeTask(exchId);

        task.event(evt, topVer);

        synchronized (mux) {
            if (!exchWorker.tasksQueue.contains(task))
                exchWorker.tasksQueue.add(task);

            mux.notifyAll();
        }
    }

    /**
     * Returns services deployment tasks with given exchange id, if task is not exists than creates new one.
     *
     * @param exchId Exchange id.
     * @return Service deployment exchange task.
     */
    private ServicesDeploymentExchangeTask exchangeTask(ServicesDeploymentExchangeId exchId) {
        ServicesDeploymentExchangeTask task = new ServicesDeploymentExchangeFutureTask(exchId);

        ServicesDeploymentExchangeTask old = tasks.putIfAbsent(exchId, task);

        return old != null ? old : task;
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
                                pendingEvts.forEach((t) -> addEvent(t.get1(), t.get2()));
                            else if (log.isDebugEnabled())
                                pendingEvts.forEach((t) -> log.debug("Ignore event, cluster is inactive: " + t.get1()));

                            pendingEvts.clear();
                        }
                        else if (msg instanceof ChangeGlobalStateMessage)
                            addEvent(evt, discoCache.version());
                        else if (msg instanceof DynamicServicesChangeRequestBatchMessage ||
                            msg instanceof DynamicCacheChangeBatch ||
                            msg instanceof CacheAffinityChangeMessage)
                            processEvent(evt, discoCache);
                        else if (msg instanceof ServicesFullMapMessage) {
                            ServicesFullMapMessage msg0 = (ServicesFullMapMessage)msg;

                            if (log.isDebugEnabled()) {
                                log.debug("Received services full map message: [locId=" + ctx.localNodeId() +
                                    ", sender=" + snd +
                                    ", msg=" + msg0 + ']');
                            }

                            ServicesDeploymentExchangeId exchId = msg0.exchangeId();

                            if (tasks.containsKey(exchId)) { // In case of double delivering
                                ServicesDeploymentExchangeTask task = exchangeTask(exchId);

                                assert task != null;

                                task.onReceiveFullMapMessage(snd, msg0);
                            }
                        }

                        break;

                    case EVT_NODE_LEFT:
                    case EVT_NODE_FAILED:

                        tasks.values().forEach(t -> t.onNodeLeft(snd));

                    case EVT_NODE_JOINED:

                        processEvent(evt, discoCache);

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

                    ServicesDeploymentExchangeTask task = exchangeTask(msg0.exchangeId());

                    assert task != null;

                    task.onReceiveSingleMapMessage(nodeId, msg0);
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
                ServicesDeploymentExchangeManagerImpl.this.log, ctx.workersRegistry());

            this.tasksQueue = new ArrayDeque<>();
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            Throwable err = null;

            try {
                ServicesDeploymentExchangeTask task;

                while (!isCancelled()) {
                    onIdle();

                    synchronized (mux) {
                        // Task shouldn't be removed from queue unless will be completed to avoid the possibility of losing
                        // event on newly joined node where the queue will be transferred.
                        task = tasksQueue.peek();

                        if (task == null) {
                            try {
                                blockingSectionBegin();

                                mux.wait();
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

                    long timeout = ctx.config().getNetworkTimeout() * 5;

                    while (true) {
                        try {
                            blockingSectionBegin();

                            task.waitForComplete(timeout);

                            taskPostProcessing(task);

                            break;
                        }
                        catch (IgniteCheckedException e) {
                            if (isCancelled)
                                return;

                            log.warning("Error occurred during waiting for exchange future completion " +
                                "or timeout had been reached, timeout=" + timeout + ", task=" + task, e);

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

            synchronized (mux) {
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
