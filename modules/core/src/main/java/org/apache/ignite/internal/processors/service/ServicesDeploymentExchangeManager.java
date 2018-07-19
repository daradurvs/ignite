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
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;

/**
 * Services deployment exchange manager.
 */
public class ServicesDeploymentExchangeManager {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final IgniteLogger log;

    /** */
    private final ServicesDeploymentExchangeWorker exchWorker;

    /** */
    private final List<ServicesSingleAssignmentsMessage> pending = new ArrayList<>();

    /** Mutex. */
    private final Object mux = new Object();

    /** */
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
     * Kernal start handler.
     */
    public void onKernalStart() {
        new IgniteThread(ctx.igniteInstanceName(), "services-deployment-exchange-worker", exchWorker).start();
    }

    /**
     * Kernal stop handler.
     */
    public void onKernalStop() {
        exchWorker.onKernalStop();

        pending.clear();
    }

    /**
     * @return Added exchange future.
     */
    public ServicesDeploymentExchangeFuture onEvent(ServicesDeploymentExchangeFuture fut) {
        synchronized (mux) {
            exchWorker.q.offer(fut);

            return fut;
        }
    }

    /**
     * @param snd Sender.
     * @param msg Services single node assignments message.
     */
    public void onReceiveSingleMessage(final UUID snd, final ServicesSingleAssignmentsMessage msg) {
        synchronized (mux) {
            ServicesDeploymentExchangeFuture fut = exchWorker.fut;

            if (fut == null) {
                pending.add(msg);

                return;
            }

            if (fut.exchangeId().equals(msg.exchId))
                fut.onReceiveSingleMessage(snd, msg, msg.client);
            else
                pending.add(msg);
        }
    }

    /**
     * @param msg Services full assignments message.
     */
    public void onReceiveFullMessage(ServicesFullAssignmentsMessage msg) {
        ServicesDeploymentExchangeFuture fut = exchWorker.fut;

        // TODO
        if (fut != null) {
            if (!fut.exchangeId().equals(msg.exchId))
                throw new IllegalStateException();

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
            while (!isCancelled()) {
                fut = q.poll();

                if (fut == null)
                    continue;

                fut.init();

                while (true) {
                    try {
                        synchronized (mux) {
                            Iterator<ServicesSingleAssignmentsMessage> it = pending.iterator();

                            while (it.hasNext()) {
                                ServicesSingleAssignmentsMessage msg = it.next();

                                if (fut.exchangeId().equals(msg.exchId)) {
                                    fut.onReceiveSingleMessage(msg.snd, msg, msg.client);

                                    it.remove();
                                }
                            }
                        }

                        fut.get(ctx.config().getNetworkTimeout() * 3);

                        break;
                    }
                    catch (IgniteCheckedException e) {
                        log.error("Exception while waiting for exchange future complete.", e);

                        if (isStopped)
                            return;
                    }
                }
            }
        }

        /**
         * Kernal stop handler.
         */
        private void onKernalStop() {
            synchronized (this) {
                isStopped = true;

                notifyAll();
            }
        }
    }
}
