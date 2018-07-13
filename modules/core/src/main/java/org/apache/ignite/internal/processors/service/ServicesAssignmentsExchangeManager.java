package org.apache.ignite.internal.processors.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

public class ServicesAssignmentsExchangeManager {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final ServicesMapExchangeWorker exchWorker;

    /**
     * @param ctx Grid kernal context.
     */
    public ServicesAssignmentsExchangeManager(GridKernalContext ctx) {
        this.ctx = ctx;
        this.exchWorker = new ServicesMapExchangeWorker(
            ctx.igniteInstanceName(),
            "services-map-exchange",
            ctx.log(getClass())
        );

        new IgniteThread(exchWorker).start();
    }

    /**
     * @param topVer Topology version.
     * @return Added exchange future.
     */
    public synchronized ServicesAssignmentsExchangeFuture onEvent(ServicesAssignmentsExchangeFuture fut,
        AffinityTopologyVersion topVer) {
        Collection<ClusterNode> nodes = ctx.discovery().nodes(topVer.topologyVersion());

//        Set<UUID> remaining = nodes.stream().filter(e -> !e.isClient()).map(ClusterNode::id).collect(Collectors.toSet());
        Set<UUID> remaining = nodes.stream().map(ClusterNode::id).collect(Collectors.toSet());

        fut.remaining(remaining);

        exchWorker.q.offer(fut);

        return fut;
    }

    private final Object mux = new Object();

    List<ServicesSingleAssignmentsMessage> pending = new ArrayList<>();

    /**
     * @param snd
     * @param msg
     */
    public synchronized void onReceiveSingleMessage(final UUID snd, final ServicesSingleAssignmentsMessage msg) {
        synchronized (mux) {
            ServicesAssignmentsExchangeFuture fut = exchWorker.fut;

            if (!fut.exchId.equals(msg.exchId))
                fut = null;

            if (fut == null) {
                for (ServicesAssignmentsExchangeFuture f : exchWorker.q) {
                    if (f.exchId.equals(msg.exchId)) {
                        fut = f;

                        break;
                    }
                }
            }

            if (fut == null) {
                pending.add(msg);

                return;
            }

            fut.onReceiveSingleMessage(snd, msg, ctx.discovery().node(snd).isClient());
        }
    }

    List<ServicesFullAssignmentsMessage> pendingFull = new ArrayList<>();

    public void onReceiveFullMessage(ServicesFullAssignmentsMessage msg) {
//        if (ctx.clientNode()) {
            ServicesAssignmentsExchangeFuture fut = exchWorker.fut;

            if (!fut.exchId.equals(msg.exchId))
                fut = null;

            if (fut == null) {
                for (ServicesAssignmentsExchangeFuture f : exchWorker.q) {
                    if (f.exchId.equals(msg.exchId)) {
                        fut = f;

                        break;
                    }
                }
            }

            if (fut != null)
                fut.onDone();
            else
                pendingFull.add(msg);

//        }
    }

    /** */
    private class ServicesMapExchangeWorker extends GridWorker {
        /** */
        private final LinkedBlockingQueue<ServicesAssignmentsExchangeFuture> q = new LinkedBlockingQueue<>();

        volatile ServicesAssignmentsExchangeFuture fut = null;

        /** {@inheritDoc} */
        protected ServicesMapExchangeWorker(@Nullable String igniteInstanceName, String name,
            IgniteLogger log) {
            super(igniteInstanceName, name, log);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            while (!isCancelled()) {
                fut = q.poll();

                if (fut == null)
                    continue;

                fut.init();

//                if (!ctx.clientNode()) {
                    for (ServicesSingleAssignmentsMessage msg : pending) {
                        if (fut.exchId.equals(msg.exchId))
                            fut.onReceiveSingleMessage(msg.snd, msg, msg.client);
                    }
//                }
//                else {
                    if (pendingFull.stream().anyMatch(msg -> fut.exchId.equals(msg.exchId))) {
                        fut.onDone();

                        continue;
                    }
//                }

                try {
                    fut.get();
                }
                catch (IgniteCheckedException e) {
                    e.printStackTrace();

                    throw U.convertException(e);
                }
            }
        }
    }
}
