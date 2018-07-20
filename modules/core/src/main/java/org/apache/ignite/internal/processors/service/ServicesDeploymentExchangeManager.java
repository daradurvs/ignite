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
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;

import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;

/**
 * Services deployment exchange manager.
 */
public class ServicesDeploymentExchangeManager {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Exchange worker. */
    private final ServicesDeploymentExchangeWorker exchWorker;

    /** Pending messages. */
    private final List<ServicesSingleAssignmentsMessage> pendingMsgs = new ArrayList<>();

    /** Mutex. */
    private final Object mux = new Object();

    /** Indicates that worker is stopped. */
    private volatile boolean isStopped = false;

    /**
     * @param ctx Grid kernal context.
     */
    public ServicesDeploymentExchangeManager(GridKernalContext ctx) {
        this.ctx = ctx;
        this.log = ctx.log(getClass());

        this.exchWorker = new ServicesDeploymentExchangeWorker();
    }

    /**
     * Starts work of deployment exchange manager/
     */
    public void startProcessing() {
        new IgniteThread(ctx.igniteInstanceName(), "services-deployment-exchange-worker", exchWorker).start();
    }

    /**
     * Starts work of deployment exchange manager.
     */
    public void stopProcessing() {
        exchWorker.stopProcessing();

        synchronized (mux) {
            mux.notifyAll();
        }

        if (log.isDebugEnabled() && !pendingMsgs.isEmpty())
            log.debug("Exchange manager contained pending messages: [" + pendingMsgs + ']');

        pendingMsgs.clear();
    }

    /**
     * Adds exchange future.
     */
    public boolean onEvent(ServicesDeploymentExchangeFuture fut) {
        boolean res = exchWorker.q.offer(fut);

        synchronized (mux) {
            mux.notifyAll();
        }

        return res;
    }

    /**
     * @param msg Services single node assignments message.
     */
    public void onReceiveSingleMessage(final ServicesSingleAssignmentsMessage msg) {
        synchronized (mux) {
            ServicesDeploymentExchangeFuture fut = exchWorker.fut;

            if (fut == null) {
                pendingMsgs.add(msg);

                return;
            }

            if (fut.exchangeId().equals(msg.exchangeId()))
                fut.onReceiveSingleMessage(msg);
            else
                pendingMsgs.add(msg);
        }
    }

    /**
     * @param nodeId Node id.
     */
    public void onNodeLeft(UUID nodeId) {
        synchronized (mux) {
            exchWorker.q.forEach(fut -> fut.onNodeLeft(nodeId));
        }
    }

    /**
     * @param msg Services full assignments message.
     */
    public void onReceiveFullMessage(ServicesFullAssignmentsMessage msg) {
        ServicesDeploymentExchangeFuture fut = exchWorker.fut;

        if (fut != null) {
            if (!fut.exchangeId().equals(msg.exchangeId()) && log.isDebugEnabled())
                log.error("Unexpected services full assignments message received: [msg=" + msg + ']');
            else
                fut.onDone();
        }
    }

    /**
     * Services deployment exchange worker.
     */
    private class ServicesDeploymentExchangeWorker extends GridWorker {
        /** */
        private final LinkedBlockingQueue<ServicesDeploymentExchangeFuture> q = new LinkedBlockingQueue<>();

        /** Exchange future in work. */
        volatile ServicesDeploymentExchangeFuture fut = null;

        /** {@inheritDoc} */
        protected ServicesDeploymentExchangeWorker() {
            super(ctx.igniteInstanceName(), "services-deployment-exchanger",
                ServicesDeploymentExchangeManager.this.log, ctx.workersRegistry());
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            Throwable err = null;

            try {
                body0();
            }
            catch (IgniteInterruptedCheckedException e) {
                if (!isStopped)
                    err = e;
            }
            catch (Throwable e) {
                err = e;
            }
            finally {
                if (err == null && !isStopped)
                    err = new IllegalStateException("Thread " + name() + " is terminated unexpectedly");

                if (err instanceof OutOfMemoryError)
                    ctx.failure().process(new FailureContext(CRITICAL_ERROR, err));
                else if (err != null)
                    ctx.failure().process(new FailureContext(SYSTEM_WORKER_TERMINATION, err));
            }
        }

        /**
         * @throws IgniteInterruptedCheckedException If interrupted.
         */
        protected void body0() throws IgniteInterruptedCheckedException {
            while (!isCancelled()) {
                fut = q.poll();

                synchronized (mux) {
                    if (fut == null) {
                        U.wait(mux);

                        continue;
                    }
                }

                try {
                    fut.init();
                }
                catch (Exception e) {
                    log.error("Failed to init services exchange future.", e);

                    fut.onDone(e);

                    continue;
                }

                long timeout = ctx.config().getNetworkTimeout() * 5;

                while (true) {
                    try {
                        synchronized (mux) {
                            Iterator<ServicesSingleAssignmentsMessage> it = pendingMsgs.iterator();

                            while (it.hasNext()) {
                                ServicesSingleAssignmentsMessage msg = it.next();

                                if (fut.exchangeId().equals(msg.exchangeId())) {
                                    fut.onReceiveSingleMessage(msg);

                                    it.remove();
                                }
                            }
                        }

                        fut.get(timeout);

                        break;
                    }
                    catch (IgniteCheckedException e) {
                        log.error("Exception while waiting for exchange future complete or timeout had been reached, timeout=" + timeout, e);

                        if (isStopped)
                            return;
                    }
                }
            }
        }

        /**
         * Processing stop handler.
         */
        private void stopProcessing() {
            synchronized (mux) {
                isStopped = true;

                mux.notifyAll();
            }
        }
    }
}
