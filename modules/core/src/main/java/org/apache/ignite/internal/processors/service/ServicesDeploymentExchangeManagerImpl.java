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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;

/**
 * Services deployment exchange manager.
 */
public class ServicesDeploymentExchangeManagerImpl implements ServicesDeploymentExchangeManager {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Exchange worker. */
    private final ServicesDeploymentExchangeWorker exchWorker;

    /** Indicates that worker is stopped. */
    private volatile boolean isStopped = true;

    /** Topology version of latest deployment task's event. */
    private final AtomicReference<AffinityTopologyVersion> readyTopVer =
        new AtomicReference<>(AffinityTopologyVersion.NONE);

    /**
     * @param ctx Grid kernal context.
     */
    public ServicesDeploymentExchangeManagerImpl(GridKernalContext ctx) {
        this.ctx = ctx;
        this.log = ctx.log(getClass());

        this.exchWorker = new ServicesDeploymentExchangeWorker();
    }

    /** {@inheritDoc} */
    @Override public void startProcessing() {
        isStopped = false;

        new IgniteThread(ctx.igniteInstanceName(), "services-deployment-exchange-worker", exchWorker).start();
    }

    /** {@inheritDoc} */
    @Override public void stopProcessing() {
        try {
            isStopped = true;

            U.cancel(exchWorker);

            U.join(exchWorker, log);

            exchWorker.tasksQueue.forEach(t -> t.complete(null, true));

            exchWorker.tasksQueue.clear();
        }
        catch (Exception e) {
            log.error("Error occurred during stopping exchange worker.");
        }
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion readyTopologyVersion() {
        return readyTopVer.get();
    }

    /** {@inheritDoc} */
    @Override public void processEvent(DiscoveryEvent evt, AffinityTopologyVersion topVer) {
        ServicesDeploymentExchangeId exchId = new ServicesDeploymentExchangeId(evt, topVer);

//        ServicesDeploymentExchangeTask task = new ServicesDeploymentExchangeFutureTask(ctx, evt, topVer, exchId);
        ServicesDeploymentExchangeTask task = exchangeTask(exchId);

        task.event(evt, topVer);

        if (evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED)
            onNodeLeft(evt.eventNode().id());

        if (!exchWorker.tasksQueue.contains(task))
            exchWorker.tasksQueue.offer(task);
    }

    /** {@inheritDoc} */
    @Override public void onReceiveSingleMapMessage(UUID snd, ServicesSingleMapMessage msg) {
        ServicesDeploymentExchangeTask task = exchangeTask(msg.exchangeId());

        assert task != null;

        task.onReceiveSingleMapMessage(snd, msg);
    }

    /** {@inheritDoc} */
    @Override public void onReceiveFullMapMessage(UUID snd, ServicesFullMapMessage msg) {
        ServicesDeploymentExchangeTask task = exchangeTask(msg.exchangeId());

        assert task != null;

        task.onReceiveFullMapMessage(snd, msg);

        tasks.remove(msg.exchangeId());
//
//        ServicesDeploymentExchangeTask fut;
//
//        if (!isStopped)
//            fut = exchWorker.task;
//        else
//            fut = exchWorker.tasksQueue.peek();
//
//        if (fut != null) {
//            if (!fut.exchangeId().equals(msg.exchangeId())) {
//                log.warning("Unexpected services full assignments message received" +
//                    ", locId=" + ctx.localNodeId() +
//                    ", msg=" + msg);
//
//                // The section to handle a critical situation when TcpDiscoverySpi breaches safeguards.
//                if (isStopped) {
//                    boolean found = false;
//
//                    for (ServicesDeploymentExchangeTask f : exchWorker.tasksQueue) {
//                        if (f.exchangeId().equals(msg.exchangeId())) {
//                            found = true;
//
//                            break;
//                        }
//                    }
//
//                    if (found) {
//                        do {
//                            fut = exchWorker.tasksQueue.poll();
//
//                            if (fut != null)
//                                fut.onReceiveFullMapMessage(snd, msg);
//                        }
//                        while (fut != null && !fut.exchangeId().equals(msg.exchangeId()));
//                    }
//                }
//            }
//            else {
//                fut.onReceiveFullMapMessage(snd, msg);
//
//                if (isStopped)
//                    exchWorker.tasksQueue.poll();
//            }
//        }
    }

    /** {@inheritDoc} */
    @Override public void onNodeLeft(UUID nodeId) {
//        if (isStopped)
//            return;

        if (exchWorker.task != null)
            exchWorker.task.onNodeLeft(nodeId);
    }

    /** {@inheritDoc} */
    @Nullable public ServicesDeploymentExchangeTask task(ServicesDeploymentExchangeId exchId) {
        ServicesDeploymentExchangeTask task = exchWorker.task;

        if (task != null && task.exchangeId().equals(exchId))
            return task;

        for (ServicesDeploymentExchangeTask t : exchWorker.tasksQueue) {
            if (t.exchangeId().equals(exchId))
                return t;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public synchronized void insertFirst(LinkedBlockingDeque<ServicesDeploymentExchangeTask> tasks) {
        tasks.descendingIterator().forEachRemaining(task -> {
            if (!exchWorker.tasksQueue.contains(task))
                exchWorker.tasksQueue.addFirst(task);
        });
    }

    /** {@inheritDoc} */
    @Override public LinkedBlockingDeque<ServicesDeploymentExchangeTask> tasks() {
        return new LinkedBlockingDeque<>(exchWorker.tasksQueue);
    }

    private Map<ServicesDeploymentExchangeId, ServicesDeploymentExchangeTask> tasks = new ConcurrentHashMap<>();

    public ServicesDeploymentExchangeTask exchangeTask(ServicesDeploymentExchangeId exchId) {
        ServicesDeploymentExchangeTask task = new ServicesDeploymentExchangeFutureTask(ctx, exchId);

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
                    task.init();
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
                        log.error("Error occurred during waiting for exchange future completion " +
                            "or timeout had been reached, timeout=" + timeout + ", task=" + task, e);

                        if (task.isComplete()) {
                            taskPostProcessing(task);

                            break;
                        }

                        if (isCancelled)
                            return;

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
}
