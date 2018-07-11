package org.apache.ignite.internal.processors.service;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;

import static org.apache.ignite.internal.GridTopic.TOPIC_SERVICES;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SERVICE_POOL;

public class ServicesAssignmentsExchangeManager {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final LinkedBlockingQueue<ServicesAssignmentsExchangeFuture> q;

    /**
     * @param ctx Grid kernal context.
     */
    public ServicesAssignmentsExchangeManager(GridKernalContext ctx) {
        this.ctx = ctx;
        this.q = new LinkedBlockingQueue<>();
        ;
    }

    /**
     * @param topVer Topology version.
     * @return Added exchange future.
     */
    public synchronized ServicesAssignmentsExchangeFuture onEvent(AffinityTopologyVersion topVer) {
        ServicesAssignmentsExchangeFuture fut = new ServicesAssignmentsExchangeFuture();

        Collection<ClusterNode> nodes = ctx.discovery().serverTopologyNodes(topVer.topologyVersion());

        Set<UUID> remaining = nodes.stream().filter(e -> !e.isClient()).map(ClusterNode::id).collect(Collectors.toSet());

        fut.remaining(remaining);

        try {
            q.put(fut);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        return fut;
    }

    /**
     * @param snd
     * @param msg
     */
    public synchronized void onReceiveSingleMessage(final UUID snd, final ServicesSingleAssignmentsMessage msg) {
        ServicesAssignmentsExchangeFuture fut = q.peek();

        if (fut != null) {
            while (!fut.remaining().contains(snd)) {
                fut = q.peek();

                if (fut == null)
                    return;
            }

            fut.onReceiveSingleMessage(snd, msg);

            if (fut.remaining().isEmpty()) {
                ServicesFullAssignmentsMessage fullMapMsg = fut.createFullAssignmentsMessage();

                for (ClusterNode node : ctx.discovery().allNodes()) {
                    if (ctx.discovery().alive(node)) {
                        try {
                            ctx.io().sendToGridTopic(node, TOPIC_SERVICES, fullMapMsg, SERVICE_POOL);
                        }
                        catch (IgniteCheckedException e) {
                            e.printStackTrace();
                        }
                    }
                }

//                fut.onDone();
//
//                q.poll();
            }
        }
    }

    /**
     * @param snd
     * @param msg
     */
    public synchronized void onReceiveFullMessage(final UUID snd, final ServicesFullAssignmentsMessage msg) {
        ServicesAssignmentsExchangeFuture fut = q.poll();

        if (fut != null)
            fut.onDone();
    }
}
