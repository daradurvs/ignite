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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingDeque;
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
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.apache.ignite.internal.util.GridBoundedLinkedHashMap;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
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
    /** Discovery events types to listen. */
    private static final int[] EVTS = {EVT_NODE_JOINED, EVT_NODE_LEFT, EVT_NODE_FAILED, EVT_DISCOVERY_CUSTOM_EVT};

    /** Busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** Mutex. */
    private final Object mux = new Object();

    /** Services discovery messages listener. */
    private final DiscoveryEventListener discoLsnr = new ServiceDiscoveryListener();

    /** Services communication messages listener. */
    private final GridMessageListener commLsnr = new ServiceCommunicationListener();

    /** Services deploymentst tasks. */
    private Map<ServicesDeploymentExchangeId, ServicesDeploymentExchangeTask> tasks = new GridBoundedLinkedHashMap<>(5);

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

        ctx.event().addDiscoveryEventListener(discoLsnr, EVTS);

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

            U.cancel(exchWorker);

            U.join(exchWorker, log);

            tasks.values().forEach(t -> t.complete(null, true));

            tasks.clear();

            exchWorker.tasksQueue.clear();
        }
        catch (Exception e) {
            log.error("Error occurred during stopping exchange worker.");
        }
    }

    /** {@inheritDoc} */
    @Override public void insertFirst(LinkedBlockingDeque<ServicesDeploymentExchangeTask> tasks) {
        synchronized (mux) {
            tasks.descendingIterator().forEachRemaining(t -> {
                if (!exchWorker.tasksQueue.contains(t)) {
                    exchWorker.tasksQueue.addFirst(t);

                    this.tasks.put(t.exchangeId(), t);
                }
            });
        }
    }

    /** {@inheritDoc} */
    @Override public LinkedBlockingDeque<ServicesDeploymentExchangeTask> tasks() {
        return exchWorker.tasksQueue;
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion readyTopologyVersion() {
        return readyTopVer.get();
    }

    /** {@inheritDoc} */
    @Override public void onLocalJoin(DiscoveryEvent evt, DiscoCache discoCache) {
        synchronized (mux) {
            discoLsnr.onEvent(evt, discoCache);
        }
    }

    /**
     * Handles discovery event.
     *
     * @param evt Discovery event.
     * @param topVer Topology version.
     */
    private void processEvent(DiscoveryEvent evt, AffinityTopologyVersion topVer) {
        ServicesDeploymentExchangeId exchId = new ServicesDeploymentExchangeId(evt, topVer);

        ServicesDeploymentExchangeTask task = exchangeTask(exchId);

        task.event(evt, topVer);

        if (!exchWorker.tasksQueue.contains(task))
            exchWorker.tasksQueue.offer(task);
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
     * Services deployment exchange worker.
     */
    private class ServicesDeploymentExchangeWorker extends GridWorker {
        /** Queue to process. */
        private final LinkedBlockingDeque<ServicesDeploymentExchangeTask> tasksQueue;

        /** Exchange future in work. */
        volatile ServicesDeploymentExchangeTask task = null;

        /** {@inheritDoc} */
        private ServicesDeploymentExchangeWorker() {
            super(ctx.igniteInstanceName(), "services-deployment-exchanger",
                ServicesDeploymentExchangeManagerImpl.this.log, ctx.workersRegistry());

            this.tasksQueue = new LinkedBlockingDeque<>();
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            Throwable err = null;

            try {
                body0();
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
         * @throws InterruptedException If interrupted.
         */
        private void body0() throws InterruptedException, IgniteCheckedException {
            while (!isCancelled()) {
                task = null;

                if (isCancelled())
                    Thread.currentThread().interrupt();

                task = tasksQueue.take();

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
                        task.waitForComplete(timeout);

                        taskPostProcessing(task);

                        break;
                    }
                    catch (IgniteCheckedException e) {
                        if (isCancelled)
                            return;

                        log.error("Error occurred during waiting for exchange future completion " +
                            "or timeout had been reached, timeout=" + timeout + ", task=" + task, e);

                        if (task.isCompleted()) {
                            taskPostProcessing(task);

                            break;
                        }

                        for (UUID uuid : task.remaining()) {
                            if (!ctx.discovery().alive(uuid))
                                task.onNodeLeft(uuid);
                        }
                    }
                }
            }
        }

        /**
         * Does additional actions after task's completion.
         */
        private void taskPostProcessing(ServicesDeploymentExchangeTask task) {
            AffinityTopologyVersion readyVer = readyTopVer.get();

            readyTopVer.compareAndSet(readyVer, task.topologyVersion());
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

                if (evt instanceof DiscoveryCustomEvent) {
                    DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)evt).customMessage();

                    if (msg instanceof DynamicServicesChangeRequestBatchMessage ||
                        msg instanceof DynamicCacheChangeBatch ||
                        msg instanceof CacheAffinityChangeMessage ||
                        msg instanceof ChangeGlobalStateMessage)
                        processEvent(evt, discoCache.version());
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

                            task.onReceiveFullMapMessage(snd, msg0);
                        }
                    }

                    return;
                }

                switch (evt.type()) {
                    case EVT_NODE_LEFT:
                    case EVT_NODE_FAILED:

                        tasks.values().forEach(t -> t.onNodeLeft(snd));

                    case EVT_NODE_JOINED:

                        processEvent(evt, discoCache.version());

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
