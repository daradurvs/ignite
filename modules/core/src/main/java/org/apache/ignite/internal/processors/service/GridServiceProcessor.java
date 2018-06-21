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

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.binary.MetadataUpdateAcceptedMessage;
import org.apache.ignite.internal.processors.cache.binary.MetadataUpdateProposedMessage;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDeploymentException;
import org.apache.ignite.services.ServiceDescriptor;
import org.apache.ignite.thread.IgniteThreadFactory;
import org.apache.ignite.thread.OomExceptionHandler;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SERVICES_COMPATIBILITY_MODE;
import static org.apache.ignite.IgniteSystemProperties.getString;
import static org.apache.ignite.configuration.DeploymentMode.ISOLATED;
import static org.apache.ignite.configuration.DeploymentMode.PRIVATE;
import static org.apache.ignite.internal.GridTopic.TOPIC_SERVICES;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SERVICES_COMPATIBILITY_MODE;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SERVICE_POOL;

/**
 * Grid service processor.
 */
@SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter", "ConstantConditions"})
public class GridServiceProcessor extends GridProcessorAdapter implements IgniteChangeGlobalStateSupport {
    /** */
    private final Boolean srvcCompatibilitySysProp;

    /** Time to wait before reassignment retries. */
    private static final long RETRY_TIMEOUT = 1000;

    /** */
    private static final int[] EVTS = {
        EventType.EVT_NODE_JOINED,
        EventType.EVT_NODE_LEFT,
        EventType.EVT_NODE_FAILED,
        DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT
    };

    /** Local service instances. */
    private final Map<String, Collection<ServiceContextImpl>> locSvcs = new HashMap<>();

    /** Deployment futures. */
    private final ConcurrentMap<String, GridServiceDeploymentFuture> depFuts = new ConcurrentHashMap<>();

    /** Deployment futures. */
    private final ConcurrentMap<String, GridServiceDeploymentFuture> undepFuts = new ConcurrentHashMap<>();

    /** Deployment executor service. */
    private volatile ExecutorService depExe;

    /** Busy lock. */
    private volatile GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** Uncaught exception handler for thread pools. */
    private final UncaughtExceptionHandler oomeHnd = new OomExceptionHandler(ctx);

    /** Thread factory. */
    private ThreadFactory threadFactory = new IgniteThreadFactory(ctx.igniteInstanceName(), "service",
        oomeHnd);

    /** Thread local for service name. */
    private ThreadLocal<String> svcName = new ThreadLocal<>();

    /** Topology listener. */
    private DiscoveryEventListener topLsnr = new TopologyListener();

    /** Services messages discovery listener. */
    private final ServiceDeploymentListener discoLsnr = new ServiceDeploymentListener();

    /** Services meassages communication listener. */
    private final ServiceDeploymentResultListener commLsnr = new ServiceDeploymentResultListener();

    /** Contains all services assignments, not only locally deployed. */
    private final Map<String, GridServiceAssignments> svcAssigns = new ConcurrentHashMap<>();

    /**
     * @param ctx Kernal context.
     */
    public GridServiceProcessor(GridKernalContext ctx) {
        super(ctx);

        depExe = Executors.newSingleThreadExecutor(new IgniteThreadFactory(ctx.igniteInstanceName(),
            "srvc-deploy", oomeHnd));

        String servicesCompatibilityMode = getString(IGNITE_SERVICES_COMPATIBILITY_MODE);

        srvcCompatibilitySysProp = servicesCompatibilityMode == null ? null : Boolean.valueOf(servicesCompatibilityMode);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.addNodeAttribute(ATTR_SERVICES_COMPATIBILITY_MODE, srvcCompatibilitySysProp);

        if (ctx.isDaemon())
            return;

        IgniteConfiguration cfg = ctx.config();

        DeploymentMode depMode = cfg.getDeploymentMode();

        if (cfg.isPeerClassLoadingEnabled() && (depMode == PRIVATE || depMode == ISOLATED) &&
            !F.isEmpty(cfg.getServiceConfiguration()))
            throw new IgniteCheckedException("Cannot deploy services in PRIVATE or ISOLATED deployment mode: " + depMode);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        if (ctx.isDaemon() || !active)
            return;

        onKernalStart0();
    }

    /**
     * Do kernal start.
     *
     * @throws IgniteCheckedException If failed.
     */
    private void onKernalStart0() throws IgniteCheckedException {
        if (!ctx.clientNode())
            ctx.event().addDiscoveryEventListener(topLsnr, EVTS);

        ctx.discovery().setCustomEventListener(DynamicServiceChangeRequest.class, discoLsnr);

        ctx.io().addMessageListener(TOPIC_SERVICES, commLsnr);

        ServiceConfiguration[] cfgs = ctx.config().getServiceConfiguration();

        if (cfgs != null)
            deployAll(Arrays.asList(cfgs), ctx.cluster().get().forServers().predicate()).get();

        if (log.isDebugEnabled())
            log.debug("Started service processor.");
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        if (ctx.isDaemon())
            return;

        GridSpinBusyLock busyLock = this.busyLock;

        // Will not release it.
        if (busyLock != null) {
            busyLock.block();

            this.busyLock = null;
        }

        U.shutdownNow(GridServiceProcessor.class, depExe, log);

        if (!ctx.clientNode())
            ctx.event().removeDiscoveryEventListener(topLsnr);

        ctx.io().removeMessageListener(TOPIC_SERVICES, commLsnr);

        Collection<ServiceContextImpl> ctxs = new ArrayList<>();

        synchronized (locSvcs) {
            for (Collection<ServiceContextImpl> ctxs0 : locSvcs.values())
                ctxs.addAll(ctxs0);

            locSvcs.clear();
        }

        for (ServiceContextImpl ctx : ctxs) {
            ctx.setCancelled(true);

            Service svc = ctx.service();

            if (svc != null)
                try {
                    svc.cancel(ctx);
                }
                catch (Throwable e) {
                    log.error("Failed to cancel service (ignoring) [name=" + ctx.name() +
                        ", execId=" + ctx.executionId() + ']', e);

                    if (e instanceof Error)
                        throw e;
                }

            ctx.executor().shutdownNow();
        }

        for (ServiceContextImpl ctx : ctxs) {
            try {
                if (log.isInfoEnabled() && !ctxs.isEmpty())
                    log.info("Shutting down distributed service [name=" + ctx.name() + ", execId8=" +
                        U.id8(ctx.executionId()) + ']');

                ctx.executor().awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();

                U.error(log, "Got interrupted while waiting for service to shutdown (will continue stopping node): " +
                    ctx.name());
            }
        }

        Exception err = new IgniteCheckedException("Operation has been cancelled (node is stopping).");

        cancelFutures(depFuts, err);
        cancelFutures(undepFuts, err);

        if (log.isDebugEnabled())
            log.debug("Stopped service processor.");
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Activate service processor [nodeId=" + ctx.localNodeId() +
                " topVer=" + ctx.discovery().topologyVersionEx() + " ]");

        busyLock = new GridSpinBusyLock();

        depExe = Executors.newSingleThreadExecutor(new IgniteThreadFactory(ctx.igniteInstanceName(),
            "srvc-deploy", oomeHnd));

        start();

        onKernalStart0();
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        if (log.isDebugEnabled())
            log.debug("DeActivate service processor [nodeId=" + ctx.localNodeId() +
                " topVer=" + ctx.discovery().topologyVersionEx() + " ]");

        cancelFutures(depFuts, new IgniteCheckedException("Failed to deploy service, cluster in active."));

        cancelFutures(undepFuts, new IgniteCheckedException("Failed to undeploy service, cluster in active."));

        onKernalStop(true);
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        cancelFutures(depFuts, new IgniteClientDisconnectedCheckedException(ctx.cluster().clientReconnectFuture(),
            "Failed to deploy service, client node disconnected."));

        cancelFutures(undepFuts, new IgniteClientDisconnectedCheckedException(ctx.cluster().clientReconnectFuture(),
            "Failed to undeploy service, client node disconnected."));
    }

    /**
     * @param futs Futs.
     * @param err Exception.
     */
    private void cancelFutures(ConcurrentMap<String, ? extends GridFutureAdapter<?>> futs, Exception err) {
        for (Map.Entry<String, ? extends GridFutureAdapter<?>> entry : futs.entrySet()) {
            GridFutureAdapter fut = entry.getValue();

            fut.onDone(err);

            futs.remove(entry.getKey(), fut);
        }
    }

    /**
     * Validates service configuration.
     *
     * @param c Service configuration.
     * @throws IgniteException If validation failed.
     */
    private void validate(ServiceConfiguration c) throws IgniteException {
        IgniteConfiguration cfg = ctx.config();

        DeploymentMode depMode = cfg.getDeploymentMode();

        if (cfg.isPeerClassLoadingEnabled() && (depMode == PRIVATE || depMode == ISOLATED))
            throw new IgniteException("Cannot deploy services in PRIVATE or ISOLATED deployment mode: " + depMode);

        ensure(c.getName() != null, "getName() != null", null);
        ensure(c.getTotalCount() >= 0, "getTotalCount() >= 0", c.getTotalCount());
        ensure(c.getMaxPerNodeCount() >= 0, "getMaxPerNodeCount() >= 0", c.getMaxPerNodeCount());
        ensure(c.getService() != null, "getService() != null", c.getService());
        ensure(c.getTotalCount() > 0 || c.getMaxPerNodeCount() > 0,
            "c.getTotalCount() > 0 || c.getMaxPerNodeCount() > 0", null);
    }

    /**
     * @param cond Condition.
     * @param desc Description.
     * @param v Value.
     */
    private void ensure(boolean cond, String desc, @Nullable Object v) {
        if (!cond)
            if (v != null)
                throw new IgniteException("Service configuration check failed (" + desc + "): " + v);
            else
                throw new IgniteException("Service configuration check failed (" + desc + ")");
    }

    /**
     * @param name Service name.
     * @param svc Service.
     * @return Future.
     */
    public IgniteInternalFuture<?> deployNodeSingleton(ClusterGroup prj, String name, Service svc) {
        return deployMultiple(prj, name, svc, 0, 1);
    }

    /**
     * @param name Service name.
     * @param svc Service.
     * @return Future.
     */
    public IgniteInternalFuture<?> deployClusterSingleton(ClusterGroup prj, String name, Service svc) {
        return deployMultiple(prj, name, svc, 1, 1);
    }

    /**
     * @param name Service name.
     * @param svc Service.
     * @param totalCnt Total count.
     * @param maxPerNodeCnt Max per-node count.
     * @return Future.
     */
    public IgniteInternalFuture<?> deployMultiple(ClusterGroup prj, String name, Service svc, int totalCnt,
        int maxPerNodeCnt) {
        ServiceConfiguration cfg = new ServiceConfiguration();

        cfg.setName(name);
        cfg.setService(svc);
        cfg.setTotalCount(totalCnt);
        cfg.setMaxPerNodeCount(maxPerNodeCnt);

        return deployAll(prj, Collections.singleton(cfg));
    }

    /**
     * @param name Service name.
     * @param svc Service.
     * @param cacheName Cache name.
     * @param affKey Affinity key.
     * @return Future.
     */
    public IgniteInternalFuture<?> deployKeyAffinitySingleton(String name, Service svc, String cacheName,
        Object affKey) {
        A.notNull(affKey, "affKey");

        ServiceConfiguration cfg = new ServiceConfiguration();

        cfg.setName(name);
        cfg.setService(svc);
        cfg.setCacheName(cacheName);
        cfg.setAffinityKey(affKey);
        cfg.setTotalCount(1);
        cfg.setMaxPerNodeCount(1);

        // Ignore projection here.
        return deployAll(Collections.singleton(cfg), null);
    }

    /**
     * @param cfgs Service configurations.
     * @param dfltNodeFilter Default NodeFilter.
     * @return Configurations to deploy.
     */
    private PreparedConfigurations prepareServiceConfigurations(Collection<ServiceConfiguration> cfgs,
        IgnitePredicate<ClusterNode> dfltNodeFilter) {
        List<ServiceConfiguration> cfgsCp = new ArrayList<>(cfgs.size());

        List<GridServiceDeploymentFuture> failedFuts = null;
        try {

            Marshaller marsh = ctx.config().getMarshaller();

            for (ServiceConfiguration cfg : cfgs) {
                Exception err = null;

                // Deploy to projection node by default
                // or only on server nodes if no projection .
                if (cfg.getNodeFilter() == null && dfltNodeFilter != null)
                    cfg.setNodeFilter(dfltNodeFilter);

                try {
                    validate(cfg);
                }
                catch (Exception e) {
                    U.error(log, "Failed to validate service configuration [name=" + cfg.getName() +
                        ", srvc=" + cfg.getService() + ']', e);

                    err = e;
                }

                if (err == null) {
                    try {
                        ctx.security().authorize(cfg.getName(), SecurityPermission.SERVICE_DEPLOY, null);
                    }
                    catch (Exception e) {
                        U.error(log, "Failed to authorize service creation [name=" + cfg.getName() +
                            ", srvc=" + cfg.getService() + ']', e);

                        err = e;
                    }
                }

                if (err == null) {
                    try {
                        byte[] srvcBytes = U.marshal(marsh, cfg.getService());

                        cfgsCp.add(new LazyServiceConfiguration(cfg, srvcBytes));
                    }
                    catch (Exception e) {
                        U.error(log, "Failed to marshal service with configured marshaller [name=" + cfg.getName() +
                            ", srvc=" + cfg.getService() + ", marsh=" + marsh + "]", e);

                        err = e;
                    }
                }

                if (err != null) {
                    if (failedFuts == null)
                        failedFuts = new ArrayList<>();

                    GridServiceDeploymentFuture fut = new GridServiceDeploymentFuture(cfg);

                    fut.onDone(err);

                    failedFuts.add(fut);
                }
            }

        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return new PreparedConfigurations(cfgsCp, failedFuts);
    }

    /**
     * @param prj Grid projection.
     * @param cfgs Service configurations.
     * @return Future for deployment.
     */
    public IgniteInternalFuture<?> deployAll(ClusterGroup prj, Collection<ServiceConfiguration> cfgs) {
        if (prj == null)
            // Deploy to servers by default if no projection specified.
            return deployAll(cfgs, ctx.cluster().get().forServers().predicate());
        else if (prj.predicate() == F.<ClusterNode>alwaysTrue())
            return deployAll(cfgs, null);
        else
            // Deploy to predicate nodes by default.
            return deployAll(cfgs, prj.predicate());
    }

    /**
     * TODO: service deploy
     *
     * @param cfgs Service configurations.
     * @param dfltNodeFilter Default NodeFilter.
     * @return Future for deployment.
     */
    private IgniteInternalFuture<?> deployAll(Collection<ServiceConfiguration> cfgs,
        @Nullable IgnitePredicate<ClusterNode> dfltNodeFilter) {
        assert cfgs != null;

        PreparedConfigurations srvCfg = prepareServiceConfigurations(cfgs, dfltNodeFilter);

        List<ServiceConfiguration> cfgsCp = srvCfg.cfgs;

        List<GridServiceDeploymentFuture> failedFuts = srvCfg.failedFuts;

        cfgsCp.sort(Comparator.comparing(ServiceConfiguration::getName));

        GridServiceDeploymentCompoundFuture res;

        while (true) {
            res = new GridServiceDeploymentCompoundFuture();

            try {
                for (ServiceConfiguration cfg : cfgsCp) {
                    try {
                        sendToDeploy(res, cfg);
                    }
                    catch (IgniteCheckedException e) {
                        if (X.hasCause(e, ClusterTopologyCheckedException.class))
                            throw e; // Retry.
                        else
                            U.error(log, e.getMessage());
                    }
                }

                break;
            }
            catch (IgniteException | IgniteCheckedException e) {
                for (String name : res.servicesToRollback())
                    depFuts.remove(name).onDone(e);

                if (X.hasCause(e, ClusterTopologyCheckedException.class)) {
                    if (log.isDebugEnabled())
                        log.debug("Topology changed while deploying services (will retry): " + e.getMessage());
                }
                else {
                    res.onDone(new IgniteCheckedException(
                        new ServiceDeploymentException("Failed to deploy provided services.", e, cfgs)));

                    return res;
                }
            }
        }

        if (ctx.clientDisconnected()) {
            IgniteClientDisconnectedCheckedException err =
                new IgniteClientDisconnectedCheckedException(ctx.cluster().clientReconnectFuture(),
                    "Failed to deploy services, client node disconnected: " + cfgs);

            for (String name : res.servicesToRollback()) {
                GridServiceDeploymentFuture fut = depFuts.remove(name);

                if (fut != null)
                    fut.onDone(err);
            }

            return new GridFinishedFuture<>(err);
        }

        if (failedFuts != null) {
            for (GridServiceDeploymentFuture fut : failedFuts)
                res.add(fut, false);
        }

        res.markInitialized();

        return res;
    }

    /**
     * @param res Resulting compound future.
     * @param cfg Service configuration.
     * @throws IgniteCheckedException If operation failed.
     */
    private void sendToDeploy(GridServiceDeploymentCompoundFuture res, ServiceConfiguration cfg)
        throws IgniteCheckedException {
        String name = cfg.getName();

        GridServiceDeploymentFuture fut = new GridServiceDeploymentFuture(cfg);

        GridServiceDeploymentFuture old = depFuts.putIfAbsent(name, fut);

        try {
            if (old != null) {
                if (!old.configuration().equalsIgnoreNodeFilter(cfg))
                    throw new IgniteCheckedException("Failed to deploy service (service already exists with different " +
                        "configuration) [deployed=" + old.configuration() + ", new=" + cfg + ']');
                else {
                    res.add(old, false);

                    return;
                }
            }

            GridServiceAssignments oldAssign = svcAssigns.get(name);

            if (oldAssign != null) {
                if (!oldAssign.configuration().equalsIgnoreNodeFilter(cfg)) {
                    throw new IgniteCheckedException("Failed to deploy service (service already exists with " +
                        "different configuration) [deployed=" + oldAssign.configuration() + ", new=" + cfg + ']');
                }
                else
                    res.add(fut, false);
            }
            else
                res.add(fut, true);

            DynamicServiceChangeRequest req = DynamicServiceChangeRequest.deployRequest(ctx.localNodeId(), cfg);

            ctx.discovery().sendCustomEvent(req);
        }
        catch (IgniteCheckedException e) {
            fut.onDone(e);

            res.add(fut, false);

            depFuts.remove(name, fut);

            throw e;
        }
    }

    /**
     * @param name Service name.
     * @return Future.
     */
    public IgniteInternalFuture<?> cancel(String name) {
        while (true) {
            try {
                return sendToCancel(name).fut;
            }
            catch (IgniteException | IgniteCheckedException e) {
                if (X.hasCause(e, ClusterTopologyCheckedException.class)) {
                    if (log.isDebugEnabled())
                        log.debug("Topology changed while cancelling service (will retry): " + e.getMessage());
                }
                else {
                    U.error(log, "Failed to undeploy service: " + name, e);

                    return new GridFinishedFuture<>(e);
                }
            }
        }
    }

    /**
     * @return Future.
     */
    @SuppressWarnings("unchecked")
    public IgniteInternalFuture<?> cancelAll() {
        List<String> svcNames = new ArrayList<>();

        for (Map.Entry<String, GridServiceAssignments> e : svcAssigns.entrySet()) {
            GridServiceAssignments dep = e.getValue();

            svcNames.add(dep.configuration().getName());
        }

        return cancelAll(svcNames);
    }

    /**
     * @param svcNames Name of service to deploy.
     * @return Future.
     */
    @SuppressWarnings("unchecked")
    public IgniteInternalFuture<?> cancelAll(Collection<String> svcNames) {
        List<String> svcNamesCp = new ArrayList<>(svcNames);

        Collections.sort(svcNamesCp);

        GridCompoundFuture res;

        while (true) {
            res = null;

            List<String> toRollback = new ArrayList<>();

            try {
                for (String name : svcNamesCp) {
                    if (res == null)
                        res = new GridCompoundFuture<>();

                    try {
                        CancelResult cr = sendToCancel(name);

                        if (cr.rollback)
                            toRollback.add(name);

                        res.add(cr.fut);
                    }
                    catch (IgniteException | IgniteCheckedException e) {
                        if (X.hasCause(e, ClusterTopologyCheckedException.class))
                            throw e; // Retry.
                        else {
                            U.error(log, "Failed to undeploy service: " + name, e);

                            res.add(new GridFinishedFuture<>(e));
                        }
                    }
                }

                break;
            }
            catch (IgniteException | IgniteCheckedException e) {
                for (String name : toRollback)
                    undepFuts.remove(name).onDone(e);

                if (X.hasCause(e, ClusterTopologyCheckedException.class)) {
                    if (log.isDebugEnabled())
                        log.debug("Topology changed while cancelling service (will retry): " + e.getMessage());
                }
                else
                    return new GridFinishedFuture<>(e);
            }
        }

        if (res != null) {
            res.markInitialized();

            return res;
        }
        else
            return new GridFinishedFuture<>();
    }

    /**
     * @param name Name of service to remove from internal cache.
     * @return Cancellation future and a flag whether it should be completed and removed on error.
     * @throws IgniteCheckedException If operation failed.
     */
    private CancelResult sendToCancel(String name) throws IgniteCheckedException {
        try {
            ctx.security().authorize(name, SecurityPermission.SERVICE_CANCEL, null);
        }
        catch (SecurityException e) {
            return new CancelResult(new GridFinishedFuture<>(e), false);
        }

        GridServiceAssignments assign = svcAssigns.get(name);

        GridServiceDeploymentFuture fut = new GridServiceDeploymentFuture(assign.configuration());

        GridServiceDeploymentFuture old = undepFuts.putIfAbsent(name, fut);

        if (old != null)
            return new CancelResult(old, false);
        else {
            DynamicServiceChangeRequest req = DynamicServiceChangeRequest.cancelRequest(assign.nodeId(), assign.configuration());

            ctx.discovery().sendCustomEvent(req);

            // TODO: handle rollback
            return new CancelResult(fut, false);
        }
    }

    /**
     * @param name Service name.
     * @return Service topology.
     */
    public Map<UUID, Integer> serviceTopology(String name, long timeout) throws IgniteCheckedException {
        GridServiceAssignments assigns = svcAssigns.get(name);

        if (assigns == null) {
            GridServiceDeploymentFuture fut = depFuts.get(name);

            try {
                if (fut != null && !fut.isDone()) {
                    if (timeout > 0)
                        fut.get(timeout);
                    else
                        fut.get();
                }
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to gather service topology. Deployment stuck or timeout reached.");

                throw e;
            }

            assigns = svcAssigns.get(name);
        }

        if (assigns == null) {
            throw new IgniteIllegalStateException("Failed to gather service topology with given name. " +
                "Did you deploy the service?");
        }

        return assigns.assigns();
    }

    /**
     * @return Collection of service descriptors.
     */
    public Collection<ServiceDescriptor> serviceDescriptors() {
        Collection<ServiceDescriptor> descs = new ArrayList<>();

        for (Map.Entry<String, GridServiceAssignments> entry : svcAssigns.entrySet()) {
            ServiceDescriptorImpl desc = new ServiceDescriptorImpl(entry.getValue());

            descs.add(desc);
        }

        return descs;
    }

    /**
     * @param name Service name.
     * @param <T> Service type.
     * @return Service by specified service name.
     */
    @SuppressWarnings("unchecked")
    public <T> T service(String name) {
        ctx.security().authorize(name, SecurityPermission.SERVICE_INVOKE, null);

        Collection<ServiceContextImpl> ctxs;

        synchronized (locSvcs) {
            ctxs = locSvcs.get(name);
        }

        if (ctxs == null)
            return null;

        synchronized (ctxs) {
            if (ctxs.isEmpty())
                return null;

            for (ServiceContextImpl ctx : ctxs) {
                Service svc = ctx.service();

                if (svc != null)
                    return (T)svc;
            }

            return null;
        }
    }

    /**
     * @param name Service name.
     * @return Service by specified service name.
     */
    public ServiceContextImpl serviceContext(String name) {
        Collection<ServiceContextImpl> ctxs;

        synchronized (locSvcs) {
            ctxs = locSvcs.get(name);
        }

        if (ctxs == null)
            return null;

        synchronized (ctxs) {
            if (ctxs.isEmpty())
                return null;

            for (ServiceContextImpl ctx : ctxs) {
                if (ctx.service() != null)
                    return ctx;
            }

            return null;
        }
    }

    /**
     * @param prj Grid projection.
     * @param name Service name.
     * @param svcItf Service class.
     * @param sticky Whether multi-node request should be done.
     * @param timeout If greater than 0 limits service acquire time. Cannot be negative.
     * @param <T> Service interface type.
     * @return The proxy of a service by its name and class.
     * @throws IgniteException If failed to create proxy.
     */
    @SuppressWarnings("unchecked")
    public <T> T serviceProxy(ClusterGroup prj, String name, Class<? super T> svcItf, boolean sticky, long timeout)
        throws IgniteException {
        ctx.security().authorize(name, SecurityPermission.SERVICE_INVOKE, null);

        if (hasLocalNode(prj)) {
            ServiceContextImpl ctx = serviceContext(name);

            if (ctx != null) {
                Service svc = ctx.service();

                if (svc != null) {
                    if (!svcItf.isAssignableFrom(svc.getClass()))
                        throw new IgniteException("Service does not implement specified interface [svcItf=" +
                            svcItf.getName() + ", svcCls=" + svc.getClass().getName() + ']');

                    return (T)svc;
                }
            }
        }

        return new GridServiceProxy<T>(prj, name, svcItf, sticky, timeout, ctx).proxy();
    }

    /**
     * @param prj Grid nodes projection.
     * @return Whether given projection contains any local node.
     */
    private boolean hasLocalNode(ClusterGroup prj) {
        for (ClusterNode n : prj.nodes()) {
            if (n.isLocal())
                return true;
        }

        return false;
    }

    /**
     * @param name Service name.
     * @param <T> Service type.
     * @return Services by specified service name.
     */
    @SuppressWarnings("unchecked")
    public <T> Collection<T> services(String name) {
        ctx.security().authorize(name, SecurityPermission.SERVICE_INVOKE, null);

        Collection<ServiceContextImpl> ctxs;

        synchronized (locSvcs) {
            ctxs = locSvcs.get(name);
        }

        if (ctxs == null)
            return null;

        synchronized (ctxs) {
            Collection<T> res = new ArrayList<>(ctxs.size());

            for (ServiceContextImpl ctx : ctxs) {
                Service svc = ctx.service();

                if (svc != null)
                    res.add((T)svc);
            }

            return res;
        }
    }

    /**
     * Reassigns service to nodes.
     *
     * @param cfg Service configuration.
     * @param nodeId Deployment initiator id.
     * @param topVer Topology version.
     * @throws IgniteCheckedException If failed.
     */
    private void reassign(ServiceConfiguration cfg, UUID nodeId,
        AffinityTopologyVersion topVer) throws IgniteCheckedException {
        Object nodeFilter = cfg.getNodeFilter();

        if (nodeFilter != null)
            ctx.resource().injectGeneric(nodeFilter);

        int totalCnt = cfg.getTotalCount();
        int maxPerNodeCnt = cfg.getMaxPerNodeCount();
        String cacheName = cfg.getCacheName();
        Object affKey = cfg.getAffinityKey();

        while (true) {
            GridServiceAssignments assigns = new GridServiceAssignments(cfg, nodeId, topVer.topologyVersion());

            Collection<ClusterNode> nodes;

            // Call node filter outside of transaction.
            if (affKey == null) {
                nodes = ctx.discovery().nodes(topVer);

                if (assigns.nodeFilter() != null) {
                    Collection<ClusterNode> nodes0 = new ArrayList<>();

                    for (ClusterNode node : nodes) {
                        if (assigns.nodeFilter().apply(node))
                            nodes0.add(node);
                    }

                    nodes = nodes0;
                }
            }
            else
                nodes = null;

            try {
                String name = cfg.getName();

                GridServiceAssignments oldAssigns = svcAssigns.get(name);

                Map<UUID, Integer> cnts = new HashMap<>();

                if (affKey != null) {
                    ClusterNode n = ctx.affinity().mapKeyToNode(cacheName, affKey, topVer);

                    if (n != null) {
                        int cnt = maxPerNodeCnt == 0 ? totalCnt == 0 ? 1 : totalCnt : maxPerNodeCnt;

                        cnts.put(n.id(), cnt);
                    }
                }
                else {
                    if (!nodes.isEmpty()) {
                        int size = nodes.size();

                        int perNodeCnt = totalCnt != 0 ? totalCnt / size : maxPerNodeCnt;
                        int remainder = totalCnt != 0 ? totalCnt % size : 0;

                        if (perNodeCnt >= maxPerNodeCnt && maxPerNodeCnt != 0) {
                            perNodeCnt = maxPerNodeCnt;
                            remainder = 0;
                        }

                        for (ClusterNode n : nodes)
                            cnts.put(n.id(), perNodeCnt);

                        assert perNodeCnt >= 0;
                        assert remainder >= 0;

                        if (remainder > 0) {
                            int cnt = perNodeCnt + 1;

                            if (oldAssigns != null) {
                                Collection<UUID> used = new HashSet<>();

                                // Avoid redundant moving of services.
                                for (Map.Entry<UUID, Integer> e : oldAssigns.assigns().entrySet()) {
                                    // Do not assign services to left nodes.
                                    if (ctx.discovery().node(e.getKey()) == null)
                                        continue;

                                    // If old count and new count match, then reuse the assignment.
                                    if (e.getValue() == cnt) {
                                        cnts.put(e.getKey(), cnt);

                                        used.add(e.getKey());

                                        if (--remainder == 0)
                                            break;
                                    }
                                }

                                if (remainder > 0) {
                                    List<Map.Entry<UUID, Integer>> entries = new ArrayList<>(cnts.entrySet());

                                    // Randomize.
                                    Collections.shuffle(entries);

                                    for (Map.Entry<UUID, Integer> e : entries) {
                                        // Assign only the ones that have not been reused from previous assignments.
                                        if (!used.contains(e.getKey())) {
                                            if (e.getValue() < maxPerNodeCnt || maxPerNodeCnt == 0) {
                                                e.setValue(e.getValue() + 1);

                                                if (--remainder == 0)
                                                    break;
                                            }
                                        }
                                    }
                                }
                            }
                            else {
                                List<Map.Entry<UUID, Integer>> entries = new ArrayList<>(cnts.entrySet());

                                // Randomize.
                                Collections.shuffle(entries);

                                for (Map.Entry<UUID, Integer> e : entries) {
                                    e.setValue(e.getValue() + 1);

                                    if (--remainder == 0)
                                        break;
                                }
                            }
                        }
                    }
                }

                assigns.assigns(cnts);

                ////
                // To be able to collect results
                depFuts.putIfAbsent(name, new GridServiceDeploymentFuture(cfg));

                DynamicServiceChangeRequest req = DynamicServiceChangeRequest.assignmentsRequest(nodeId, cfg,
                    assigns.assigns(), topVer.topologyVersion());

                ctx.discovery().sendCustomEvent(req);
                ////

                break;
            }
            catch (ClusterTopologyCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Topology changed while reassigning (will retry): " + e.getMessage());

                U.sleep(10);
            }
        }
    }

    /**
     * Redeploys local services based on assignments.
     *
     * @param assigns Assignments.
     */
    private void redeploy(GridServiceAssignments assigns) {
        if (assigns.topologyVersion() < ctx.discovery().topologyVersion()) {
            if (log.isDebugEnabled())
                log.debug("Skip outdated assignment [assigns=" + assigns +
                    ", topVer=" + ctx.discovery().topologyVersion() + ']');

            return;
        }

        String svcName = assigns.name();

        Integer assignCnt = assigns.assigns().get(ctx.localNodeId());

        if (assignCnt == null)
            assignCnt = 0;

        Collection<ServiceContextImpl> ctxs;

        synchronized (locSvcs) {
            ctxs = locSvcs.get(svcName);

            if (ctxs == null)
                locSvcs.put(svcName, ctxs = new ArrayList<>());
        }

        Collection<ServiceContextImpl> toInit = new ArrayList<>();

        synchronized (ctxs) {
            if (ctxs.size() > assignCnt) {
                int cancelCnt = ctxs.size() - assignCnt;

                cancel(ctxs, cancelCnt);
            }
            else if (ctxs.size() < assignCnt) {
                int createCnt = assignCnt - ctxs.size();

                for (int i = 0; i < createCnt; i++) {
                    ServiceContextImpl svcCtx = new ServiceContextImpl(assigns.name(),
                        UUID.randomUUID(),
                        assigns.cacheName(),
                        assigns.affinityKey(),
                        Executors.newSingleThreadExecutor(threadFactory));

                    ctxs.add(svcCtx);

                    toInit.add(svcCtx);
                }
            }
        }

        for (final ServiceContextImpl svcCtx : toInit) {
            final Service svc;

            try {
                svc = copyAndInject(assigns.configuration());

                // Initialize service.
                svc.init(svcCtx);

                svcCtx.service(svc);
            }
            catch (Throwable e) {
                U.error(log, "Failed to initialize service (service will not be deployed): " + assigns.name(), e);

                synchronized (ctxs) {
                    ctxs.removeAll(toInit);
                }

                if (e instanceof Error)
                    throw (Error)e;

                if (e instanceof RuntimeException)
                    throw (RuntimeException)e;

                return;
            }

            if (log.isInfoEnabled())
                log.info("Starting service instance [name=" + svcCtx.name() + ", execId=" +
                    svcCtx.executionId() + ']');

            // Start service in its own thread.
            final ExecutorService exe = svcCtx.executor();

            exe.execute(new Runnable() {
                @Override public void run() {
                    try {
                        svc.execute(svcCtx);
                    }
                    catch (InterruptedException | IgniteInterruptedCheckedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Service thread was interrupted [name=" + svcCtx.name() + ", execId=" +
                                svcCtx.executionId() + ']');
                    }
                    catch (IgniteException e) {
                        if (e.hasCause(InterruptedException.class) ||
                            e.hasCause(IgniteInterruptedCheckedException.class)) {
                            if (log.isDebugEnabled())
                                log.debug("Service thread was interrupted [name=" + svcCtx.name() +
                                    ", execId=" + svcCtx.executionId() + ']');
                        }
                        else {
                            U.error(log, "Service execution stopped with error [name=" + svcCtx.name() +
                                ", execId=" + svcCtx.executionId() + ']', e);
                        }
                    }
                    catch (Throwable e) {
                        log.error("Service execution stopped with error [name=" + svcCtx.name() +
                            ", execId=" + svcCtx.executionId() + ']', e);

                        if (e instanceof Error)
                            throw (Error)e;
                    }
                    finally {
                        // Suicide.
                        exe.shutdownNow();
                    }
                }
            });
        }
    }

    /**
     * @param cfg Service configuration.
     * @return Copy of service.
     * @throws IgniteCheckedException If failed.
     */
    private Service copyAndInject(ServiceConfiguration cfg) throws IgniteCheckedException {
        Marshaller m = ctx.config().getMarshaller();

        if (cfg instanceof LazyServiceConfiguration) {
            byte[] bytes = ((LazyServiceConfiguration)cfg).serviceBytes();

            Service srvc = U.unmarshal(m, bytes, U.resolveClassLoader(null, ctx.config()));

            ctx.resource().inject(srvc);

            return srvc;
        }
        else {
            Service svc = cfg.getService();

            try {
                byte[] bytes = U.marshal(m, svc);

                Service cp = U.unmarshal(m, bytes, U.resolveClassLoader(svc.getClass().getClassLoader(), ctx.config()));

                ctx.resource().inject(cp);

                return cp;
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to copy service (will reuse same instance): " + svc.getClass(), e);

                return svc;
            }
        }
    }

    /**
     * @param ctxs Contexts to cancel.
     * @param cancelCnt Number of contexts to cancel.
     */
    private void cancel(Iterable<ServiceContextImpl> ctxs, int cancelCnt) {
        for (Iterator<ServiceContextImpl> it = ctxs.iterator(); it.hasNext(); ) {
            ServiceContextImpl svcCtx = it.next();

            // Flip cancelled flag.
            svcCtx.setCancelled(true);

            // Notify service about cancellation.
            Service svc = svcCtx.service();

            if (svc != null) {
                try {
                    svc.cancel(svcCtx);
                }
                catch (Throwable e) {
                    log.error("Failed to cancel service (ignoring) [name=" + svcCtx.name() +
                        ", execId=" + svcCtx.executionId() + ']', e);

                    if (e instanceof Error)
                        throw e;
                }
                finally {
                    try {
                        ctx.resource().cleanup(svc);
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to clean up service (will ignore): " + svcCtx.name(), e);
                    }
                }
            }

            // Close out executor thread for the service.
            // This will cause the thread to be interrupted.
            svcCtx.executor().shutdownNow();

            it.remove();

            if (log.isInfoEnabled())
                log.info("Cancelled service instance [name=" + svcCtx.name() + ", execId=" +
                    svcCtx.executionId() + ']');

            if (--cancelCnt == 0)
                break;
        }
    }

    /**
     * Deployment callback.
     *
     * @param cfg Service configuration.
     * @param nodeId Deployment initiator id.
     * @param topVer Topology version.
     */
    private void onDeployment(final ServiceConfiguration cfg, final UUID nodeId, final AffinityTopologyVersion topVer) {
        // Retry forever.
        try {
            AffinityTopologyVersion newTopVer = ctx.discovery().topologyVersionEx();

            // If topology version changed, reassignment will happen from topology event.
            if (newTopVer.equals(topVer))
                reassign(cfg, nodeId, topVer);
        }
        catch (IgniteCheckedException e) {
            if (!(e instanceof ClusterTopologyCheckedException))
                log.error("Failed to do service reassignment (will retry): " + cfg.getName(), e);

            AffinityTopologyVersion newTopVer = ctx.discovery().topologyVersionEx();

            if (!newTopVer.equals(topVer)) {
                assert newTopVer.compareTo(topVer) > 0;

                // Reassignment will happen from topology event.
                return;
            }

            ctx.timeout().addTimeoutObject(new GridTimeoutObject() {
                private IgniteUuid id = IgniteUuid.randomUuid();

                private long start = System.currentTimeMillis();

                @Override public IgniteUuid timeoutId() {
                    return id;
                }

                @Override public long endTime() {
                    return start + RETRY_TIMEOUT;
                }

                @Override public void onTimeout() {
                    depExe.execute(new DepRunnable() {
                        @Override public void run0() {
                            onDeployment(cfg, nodeId, topVer);
                        }
                    });
                }
            });
        }
    }

    /**
     * Topology listener.
     */
    private class TopologyListener implements DiscoveryEventListener {
        /** */
        private volatile AffinityTopologyVersion currTopVer = null;

        /** {@inheritDoc} */
        @Override public void onEvent(final DiscoveryEvent evt, final DiscoCache discoCache) {
            GridSpinBusyLock busyLock = GridServiceProcessor.this.busyLock;

            if (busyLock == null || !busyLock.enterBusy())
                return;

            try {
                final AffinityTopologyVersion topVer;

                if (evt instanceof DiscoveryCustomEvent) {
                    DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)evt).customMessage();

                    if (msg instanceof CacheAffinityChangeMessage) {
                        if (!((CacheAffinityChangeMessage)msg).exchangeNeeded())
                            return;
                    }
                    else if (msg instanceof DynamicCacheChangeBatch) {
                        if (!((DynamicCacheChangeBatch)msg).exchangeNeeded())
                            return;
                    }
                    else
                        return;

                    if (msg instanceof MetadataUpdateProposedMessage || msg instanceof MetadataUpdateAcceptedMessage)
                        return;

                    topVer = ((DiscoveryCustomEvent)evt).affinityTopologyVersion();
                }
                else
                    topVer = new AffinityTopologyVersion((evt).topologyVersion(), 0);

                currTopVer = topVer;

                depExe.execute(new DepRunnable() {
                    @Override public void run0() {
                        // In case the cache instance isn't tracked by DiscoveryManager anymore.
                        discoCache.updateAlives(ctx.discovery());

                        ClusterNode oldest = discoCache.oldestAliveServerNode();

                        if (oldest != null && oldest.isLocal()) {
                            final Collection<GridServiceAssignments> retries = new ConcurrentLinkedQueue<>();

                            // If topology changed again, let next event handle it.
                            AffinityTopologyVersion currTopVer0 = currTopVer;

                            if (currTopVer0 != topVer) {
                                if (log.isInfoEnabled())
                                    log.info("Service processor detected a topology change during " +
                                        "assignments calculation (will abort current iteration and " +
                                        "re-calculate on the newer version): " +
                                        "[topVer=" + topVer + ", newTopVer=" + currTopVer0 + ']');

                                return;
                            }

                            for (Map.Entry<String, GridServiceAssignments> entry : svcAssigns.entrySet()) {
                                GridServiceAssignments dep = entry.getValue();

                                try {
                                    svcName.set(dep.configuration().getName());

                                    ctx.cache().context().exchange().affinityReadyFuture(topVer).get();

                                    reassign(dep.configuration(), dep.nodeId(), topVer);
                                }
                                catch (IgniteCheckedException ex) {
                                    if (!(ex instanceof ClusterTopologyCheckedException))
                                        LT.error(log, ex, "Failed to do service reassignment (will retry): " +
                                            dep.configuration().getName());

                                    retries.add(dep);
                                }
                            }

                            if (!retries.isEmpty())
                                onReassignmentFailed(topVer, retries);
                        }
                    }
                });
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /**
         * Handler for reassignment failures.
         *
         * @param topVer Topology version.
         * @param retries Retries.
         */
        private void onReassignmentFailed(final AffinityTopologyVersion topVer,
            final Collection<GridServiceAssignments> retries) {
            GridSpinBusyLock busyLock = GridServiceProcessor.this.busyLock;

            if (busyLock == null || !busyLock.enterBusy())
                return;

            try {
                // If topology changed again, let next event handle it.
                if (ctx.discovery().topologyVersionEx().equals(topVer))
                    return;

                for (Iterator<GridServiceAssignments> it = retries.iterator(); it.hasNext(); ) {
                    GridServiceAssignments dep = it.next();

                    try {
                        svcName.set(dep.configuration().getName());

                        reassign(dep.configuration(), dep.nodeId(), topVer);

                        it.remove();
                    }
                    catch (IgniteCheckedException e) {
                        if (!(e instanceof ClusterTopologyCheckedException))
                            LT.error(log, e, "Failed to do service reassignment (will retry): " +
                                dep.configuration().getName());
                    }
                }

                if (!retries.isEmpty()) {
                    ctx.timeout().addTimeoutObject(new GridTimeoutObject() {
                        private IgniteUuid id = IgniteUuid.randomUuid();

                        private long start = System.currentTimeMillis();

                        @Override public IgniteUuid timeoutId() {
                            return id;
                        }

                        @Override public long endTime() {
                            return start + RETRY_TIMEOUT;
                        }

                        @Override public void onTimeout() {
                            depExe.execute(new Runnable() {
                                public void run() {
                                    onReassignmentFailed(topVer, retries);
                                }
                            });
                        }
                    });
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }
    }

    /** */
    private void processAssignment(String name, GridServiceAssignments val) {
        GridServiceAssignments assigns;

        try {
            assigns = val;
        }
        catch (IgniteException ex) {
            if (X.hasCause(ex, ClassNotFoundException.class))
                return;
            else
                throw ex;
        }

        svcName.set(assigns.name());

        Throwable t = null;

        try {
            redeploy(assigns);
        }
        catch (Error | RuntimeException th) {
            t = th;
        }

        try {
            ServiceDeploymentResultMessage resMsg = new ServiceDeploymentResultMessage(name);

            if (t != null) {
                resMsg.isSuccess = false;

                resMsg.errBytes = U.marshal(ctx, t);
            }
            else
                resMsg.isSuccess = true;

            // Send result to coordinator
            ClusterNode oldest = U.oldest(ctx.discovery().nodes(val.topologyVersion()), null);

            ctx.io().sendToGridTopic(oldest, TOPIC_SERVICES, resMsg, SERVICE_POOL);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * @param name Name.
     */
    private void undeploy(String name) {
        svcName.set(name);

        Collection<ServiceContextImpl> ctxs;

        synchronized (locSvcs) {
            ctxs = locSvcs.remove(name);
        }

        if (ctxs != null) {
            synchronized (ctxs) {
                cancel(ctxs, ctxs.size());
            }
        }

        // Finish deployment futures if undeployment happened.
        GridFutureAdapter<?> fut = depFuts.get(name);

        if (fut != null)
            fut.onDone();

        // Complete undeployment future.
        fut = undepFuts.remove(name);

        if (fut != null)
            fut.onDone();

        svcAssigns.remove(name);
    }

    /**
     *
     */
    private static class CancelResult {
        /** */
        IgniteInternalFuture<?> fut;

        /** */
        boolean rollback;

        /**
         * @param fut Future.
         * @param rollback {@code True} if service was cancelled during current call.
         */
        CancelResult(IgniteInternalFuture<?> fut, boolean rollback) {
            this.fut = fut;
            this.rollback = rollback;
        }
    }

    /**
     *
     */
    private abstract class DepRunnable implements Runnable {
        /** {@inheritDoc} */
        @Override public void run() {
            GridSpinBusyLock busyLock = GridServiceProcessor.this.busyLock;

            if (busyLock == null || !busyLock.enterBusy())
                return;

            // Won't block ServiceProcessor stopping process.
            busyLock.leaveBusy();

            svcName.set(null);

            try {
                run0();
            }
            catch (Throwable t) {
                log.error("Error when executing service: " + svcName.get(), t);

                if (t instanceof Error)
                    throw t;
            }
            finally {
                svcName.set(null);
            }
        }

        /**
         * Abstract run method protected by busy lock.
         */
        public abstract void run0();
    }

    /**
     * @param topVer Topology version.
     */
    private boolean isLocalNodeCoordinator(AffinityTopologyVersion topVer) {
        ClusterNode oldest = U.oldest(ctx.discovery().nodes(topVer), null);

        return oldest.isLocal();
    }

    /**
     *
     */
    private class ServiceDeploymentListener implements CustomEventListener<DynamicServiceChangeRequest> {
        /** {@inheritDoc} */
        @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd,
            DynamicServiceChangeRequest msg) {
            GridSpinBusyLock busyLock = GridServiceProcessor.this.busyLock;

            if (busyLock == null || !busyLock.enterBusy())
                return;

            try {
                depExe.execute(new DepRunnable() {
                    @Override public void run0() {
                        if (msg.isDeploy()) {

                            if (!isLocalNodeCoordinator(topVer))
                                return;

                            // Process deployment on coordinator only.
                            onDeployment(msg.configuration(), msg.nodeId(), topVer);
                        }

                        else if (msg.isAssignments()) {
                            GridServiceAssignments assigns = new GridServiceAssignments(msg.configuration(),
                                msg.nodeId(), msg.topologyVersion());

                            assigns.assigns(msg.assignments());

                            String name = assigns.name();

                            svcAssigns.put(name, assigns);

                            processAssignment(name, assigns);
                        }

                        else if (msg.isCancel()) {
                            String name = msg.name();

                            GridServiceDeploymentFuture fut = undepFuts.get(name);

                            try {
                                undeploy(name);

                                if (fut != null) {
                                    int res = fut.cntr.decrementAndGet();

                                    if (res <= 0) {
                                        fut.onDone();

                                        undepFuts.remove(name);
                                    }
                                }
                            }
                            catch (Exception e) {
                                undepFuts.remove(name, fut);

                                fut.onDone(e);

                                throw e;
                            }
                        }

                        else
                            throw new IllegalStateException("Unexpected message's goal.");
                    }
                });
            }
            finally {
                busyLock.leaveBusy();
            }
        }
    }

    /** */
    private class ServiceDeploymentResultListener implements GridMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
            if (busyLock == null || !busyLock.enterBusy())
                return;

            assert msg instanceof ServiceDeploymentResultMessage;

            ServiceDeploymentResultMessage depMsg = (ServiceDeploymentResultMessage)msg;

            String name = depMsg.name;

            try {
                GridServiceDeploymentFuture fut = depFuts.get(name);

                if (depMsg.toInitiator) {
                    if (fut != null) {
                        if (depMsg.isSuccess)
                            fut.onDone();
                        else {
                            Throwable t = U.unmarshal(ctx, depMsg.errBytes, null);

                            fut.onDone(new ServiceDeploymentException(t, Collections.singleton(fut.configuration())));
                        }
                    }

                    depFuts.remove(name, fut);
                }
                else {
                    GridServiceAssignments assigns = svcAssigns.get(name);

                    // Handle undeploy while reassignment
                    if (!assigns.assigns().containsKey(nodeId))
                        return;

                    if (fut != null) {
                        // Coordinator should collect deployment results from assigned nodes.
                        int res = fut.cntr.decrementAndGet();

                        if (!depMsg.isSuccess)
                            fut.errors.put(nodeId, depMsg.errBytes);

                        if (res <= 0) {

                            if (depMsg.isSuccess)
                                fut.onDone();
                            else {
                                byte[] errBytes = fut.errors.entrySet().iterator().next().getValue();

                                Throwable t = U.unmarshal(ctx, errBytes, null);

                                fut.onDone(new ServiceDeploymentException(t, Collections.singleton(fut.configuration())));
                            }

                            // Notify initiator
                            ServiceDeploymentResultMessage resMsg = new ServiceDeploymentResultMessage();
                            resMsg.name = name;
                            resMsg.toInitiator = true;

                            if (!fut.errors.isEmpty()) {
                                resMsg.isSuccess = false;

                                resMsg.errBytes = fut.errors.entrySet().iterator().next().getValue();
                            }
                            else
                                resMsg.isSuccess = true;

                            ctx.io().sendToGridTopic(assigns.nodeId(), TOPIC_SERVICES, resMsg, SERVICE_POOL);

                            depFuts.remove(name);
                        }
                    }
                }
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
            finally {
                busyLock.leaveBusy();
            }
        }
    }
}
