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

import java.io.Serializable;
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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeRequest;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDeploymentException;
import org.apache.ignite.services.ServiceDescriptor;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.thread.IgniteThreadFactory;
import org.apache.ignite.thread.OomExceptionHandler;
import org.jetbrains.annotations.Nullable;

import static javax.cache.event.EventType.REMOVED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SERVICES_COMPATIBILITY_MODE;
import static org.apache.ignite.IgniteSystemProperties.getString;
import static org.apache.ignite.configuration.DeploymentMode.ISOLATED;
import static org.apache.ignite.configuration.DeploymentMode.PRIVATE;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.SERVICE_PROC;
import static org.apache.ignite.internal.GridTopic.TOPIC_SERVICES;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SERVICES_COMPATIBILITY_MODE;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SERVICE_POOL;

/**
 * Grid service processor.
 */
@SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter", "ConstantConditions"})
public class GridServiceProcessor extends GridProcessorAdapter implements IgniteChangeGlobalStateSupport {
    /** Services compatibility system property. */
    private final Boolean srvcCompatibilitySysProp;

    /** Discovery events to handle. */
    private static final int[] EVTS = {EVT_NODE_JOINED, EVT_NODE_LEFT, EVT_NODE_FAILED, EVT_DISCOVERY_CUSTOM_EVT};

    /** Local services instances. */
    private final Map<String, Collection<ServiceContextImpl>> locSvcs = new HashMap<>();

    /** Deployment futures. */
    private final ConcurrentMap<String, GridServiceDeploymentFuture> depFuts = new ConcurrentHashMap<>();

    /** Deployment futures. */
    private final ConcurrentMap<String, GridFutureAdapter<?>> undepFuts = new ConcurrentHashMap<>();

    /** Deployment executor service. */
    private volatile ExecutorService depExe;

    /** Busy lock. */
    private volatile GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** Uncaught exception handler for thread pools. */
    private final UncaughtExceptionHandler oomeHnd = new OomExceptionHandler(ctx);

    /** Thread factory. */
    private ThreadFactory threadFactory = new IgniteThreadFactory(ctx.igniteInstanceName(), "service", oomeHnd);

    /** Thread local for service name. */
    private ThreadLocal<String> srvcName = new ThreadLocal<>();

    /** Discovery events listener. */
    private final DiscoveryEventListener discoLsnr = new DiscoveryListener();

    /** Services messages communication listener. */
    private final CommunicationListener commLsnr = new CommunicationListener();

    /** Contains all services assignments, not only locally deployed. */
    private final Map<String, GridServiceAssignments> srvcsAssigns = new ConcurrentHashMap<>();

    /** Services deployment exchange manager. */
    private final ServicesDeploymentExchangeManager exchangeMgr = new ServicesDeploymentExchangeManagerImpl(ctx);

    /** Services assignments function. */
    private final ServiceAssignmentsFunction assignsFunc = new ServiceAssignmentsFunctionImpl(ctx, srvcsAssigns);

    /** Mutex. */
    private final Object mux = new Object();

    /**
     * @param ctx Kernal context.
     */
    public GridServiceProcessor(GridKernalContext ctx) {
        super(ctx);

        depExe = Executors.newSingleThreadExecutor(new IgniteThreadFactory(ctx.igniteInstanceName(),
            "srvc-deploy", oomeHnd));

        String servicesCompatibilityMode = getString(IGNITE_SERVICES_COMPATIBILITY_MODE);

        srvcCompatibilitySysProp = servicesCompatibilityMode == null ? null : Boolean.valueOf(servicesCompatibilityMode);

        ctx.event().addDiscoveryEventListener(discoLsnr, EVTS);

        ctx.io().addMessageListener(TOPIC_SERVICES, commLsnr);
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
        if (!srvcsAssigns.isEmpty())
            srvcsAssigns.values().forEach(this::redeploy);

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

        exchangeMgr.stopProcessing();

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
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        if (!isLocalNodeCoordinator())
            return;

        synchronized (mux) {
            if (!dataBag.commonDataCollectedFor(SERVICE_PROC.ordinal())) {
                InitialServicesData initData = new InitialServicesData(new ArrayList<>(srvcsAssigns.values()));

                dataBag.addGridCommonData(SERVICE_PROC.ordinal(), initData);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        synchronized (mux) {
            InitialServicesData initData = (InitialServicesData)data.commonData();

            for (GridServiceAssignments assign : initData.assigns)
                srvcsAssigns.put(assign.name(), assign);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryDataExchangeType discoveryDataType() {
        return SERVICE_PROC;
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

        Marshaller marsh = ctx.config().getMarshaller();

        List<GridServiceDeploymentFuture> failedFuts = null;

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
     * @param cfgs Services configurations.
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

            synchronized (mux) {
                try {
                    List<ServiceConfiguration> cfgsToDeploy = new ArrayList<>(cfgsCp.size());

                    for (ServiceConfiguration cfg : cfgsCp) {
                        String name = cfg.getName();

                        GridServiceDeploymentFuture fut = new GridServiceDeploymentFuture(cfg);

                        GridServiceDeploymentFuture old = depFuts.putIfAbsent(name, fut);

                        ServiceConfiguration oldDifCfg = null;

                        if (old != null) {
                            if (!old.configuration().equalsIgnoreNodeFilter(cfg))
                                oldDifCfg = old.configuration();
                            else {
                                res.add(old, false);

                                continue;
                            }
                        }

                        if (oldDifCfg == null) {
                            GridServiceAssignments assign = srvcsAssigns.get(name);

                            if (assign != null && !assign.configuration().equalsIgnoreNodeFilter(cfg))
                                oldDifCfg = assign.configuration();
                        }

                        if (oldDifCfg != null) {
                            res.add(fut, false);

                            fut.onDone(new IgniteCheckedException("Failed to deploy service (service already exists with " +
                                "different configuration) [deployed=" + oldDifCfg + ", new=" + cfg + ']'));
                        }
                        else {
                            res.add(fut, true);

                            cfgsToDeploy.add(cfg);
                        }
                    }

                    if (!cfgsToDeploy.isEmpty()) {
                        ServicesDeploymentRequestMessage msg = new ServicesDeploymentRequestMessage(
                            ctx.localNodeId(), cfgsToDeploy);

                        ctx.discovery().sendCustomEvent(msg);

                        if (log.isDebugEnabled())
                            log.debug("Services have been sent to deploy, req=" + msg);
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
     * @param name Service name to cancel.
     * @return Future.
     */
    public IgniteInternalFuture<?> cancel(String name) {
        return cancelAll(Collections.singleton(name));
    }

    /**
     * @return Future.
     */
    @SuppressWarnings("unchecked")
    public IgniteInternalFuture<?> cancelAll() {
        return cancelAll(srvcsAssigns.keySet());
    }

    /**
     * @param srvcsNames Services names to cancel.
     * @return Future.
     */
    @SuppressWarnings("unchecked")
    public IgniteInternalFuture<?> cancelAll(Collection<String> srvcsNames) {
        List<String> srvcsNamesCp = new ArrayList<>(srvcsNames);

        Collections.sort(srvcsNamesCp);

        GridCompoundFuture res;

        while (true) {
            res = new GridCompoundFuture<>();

            synchronized (mux) {
                List<String> toRollback = new ArrayList<>();

                List<String> namesToCancel = new ArrayList<>(srvcsNamesCp.size());

                try {
                    for (String name : srvcsNamesCp) {
                        try {
                            ctx.security().authorize(name, SecurityPermission.SERVICE_CANCEL, null);
                        }
                        catch (SecurityException e) {
                            res.add(new GridFinishedFuture<>(e));

                            continue;
                        }

                        GridFutureAdapter<?> fut = new GridFutureAdapter<>();

                        GridFutureAdapter<?> old = undepFuts.putIfAbsent(name, fut);

                        if (old != null) {
                            res.add(old);

                            continue;
                        }

                        res.add(fut);

                        if (!srvcsAssigns.containsKey(name)) {
                            fut.onDone();

                            continue;
                        }

                        toRollback.add(name);

                        namesToCancel.add(name);
                    }

                    if (!namesToCancel.isEmpty()) {
                        ServicesCancellationRequestMessage msg = new ServicesCancellationRequestMessage(
                            ctx.localNodeId(), namesToCancel);

                        ctx.discovery().sendCustomEvent(msg);

                        if (log.isDebugEnabled())
                            log.debug("Services have been sent to cancel, msg=" + msg);
                    }

                    break;
                }
                catch (IgniteException | IgniteCheckedException e) {
                    for (String name : toRollback)
                        undepFuts.remove(name).onDone(e);

                    if (X.hasCause(e, ClusterTopologyCheckedException.class)) {
                        if (log.isDebugEnabled())
                            log.debug("Topology changed while cancelling services (will retry): " + e.getMessage());
                    }
                    else {
                        U.error(log, "Failed to undeploy services: " + srvcsNamesCp, e);

                        res.onDone(e);

                        return res;
                    }
                }
            }
        }

        res.markInitialized();

        return res;
    }

    /**
     * @param name Service name.
     * @return Service topology.
     */
    public Map<UUID, Integer> serviceTopology(String name) {
        synchronized (mux) {
            GridServiceAssignments assign = srvcsAssigns.get(name);

            if (assign == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Requested service assignments have not been found : [" + name +
                        ", locId=" + ctx.localNodeId() +
                        ", client=" + ctx.clientNode() + ']');
                }

                return null;
            }

            return assign.assigns();
        }
    }

    /**
     * @return Collection of service descriptors.
     */
    public Collection<ServiceDescriptor> serviceDescriptors() {
        synchronized (mux) {
            Collection<GridServiceAssignments> assigns = srvcsAssigns.values();

            Collection<ServiceDescriptor> descs = new ArrayList<>();

            for (GridServiceAssignments assign : assigns)
                descs.add(new ServiceDescriptorImpl(assign));

            return descs;
        }
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
     * Redeploys local services based on assignments.
     *
     * @param assigns Service assignments.
     */
    private void redeploy(GridServiceAssignments assigns) {
        redeploy(assigns.name(), assigns.configuration(), assigns.assigns(), assigns.cacheName(), assigns.affinityKey());
    }

    /**
     * Redeploys local services based on assignments.
     *
     * @param name Service name.
     * @param cfg Service configuration.
     * @param assigns Service assignments.
     * @param cacheName Cache name
     * @param affKey Affinity key.
     */
    private void redeploy(String name, ServiceConfiguration cfg, Map<UUID, Integer> assigns, String cacheName,
        Object affKey) {
        Integer assignCnt = assigns.get(ctx.localNodeId());

        if (assignCnt == null)
            assignCnt = 0;

        Collection<ServiceContextImpl> ctxs;

        synchronized (locSvcs) {
            ctxs = locSvcs.get(name);

            if (ctxs == null)
                locSvcs.put(name, ctxs = new ArrayList<>());
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
                    ServiceContextImpl svcCtx = new ServiceContextImpl(name,
                        UUID.randomUUID(),
                        cacheName,
                        affKey,
                        Executors.newSingleThreadExecutor(threadFactory));

                    ctxs.add(svcCtx);

                    toInit.add(svcCtx);
                }
            }
        }

        for (final ServiceContextImpl svcCtx : toInit) {
            final Service svc;

            try {
                svc = copyAndInject(cfg);

                // Initialize service.
                svc.init(svcCtx);

                svcCtx.service(svc);
            }
            catch (Throwable e) {
                U.error(log, "Failed to initialize service (service will not be deployed): " + name, e);

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
     * @param req Service assignments request.
     */
    private void onAssignmentsRequest(ServicesAssignmentsRequestMessage req) {
        try {
            final Collection<GridServiceAssignments> newAssigns = req.assignments();

            final Map<String, Throwable> errors = new HashMap<>();

            synchronized (mux) {
                for (GridServiceAssignments assigns : newAssigns) {
                    String name = assigns.name();

                    srvcsAssigns.putIfAbsent(name, assigns);

                    deployIfNeeded(name, assigns.assigns(), errors);
                }

                createAndSendSingleAssingmentsMessage(req.exchangeId(), errors);
            }
        }
        catch (Exception e) {
            log.error("Error occurred during processing of service assignments request, req=" + req, e);
        }
    }

    /**
     * @param req Services reassignments request.
     */
    private void onReassignmentsRequest(ServicesReassignmentsRequestMessage req) {
        try {
            final Map<String, Throwable> errors = new HashMap<>();

            synchronized (mux) {
                req.assigns().forEach((name, assigns) -> deployIfNeeded(name, assigns, errors));

                req.servicesToUndeploy().forEach(this::undeploy);

                createAndSendSingleAssingmentsMessage(req.exchangeId(), errors);
            }
        }
        catch (Exception e) {
            log.error("Error occurred during processing of service reassignments request, req=" + req, e);
        }
    }

    /**
     * Deploys service with given name if a number of local instances less than its number in given assignments.
     *
     * @param name Service name.
     * @param assigns Service assigns.
     * @param errors Deployment errors container to fill in.
     */
    private void deployIfNeeded(String name, Map<UUID, Integer> assigns, Map<String, Throwable> errors) {
        Integer expNum = assigns.get(ctx.localNodeId());

        boolean needDeploy = false;

        if (expNum != null && expNum > 0) {
            Collection<ServiceContextImpl> ctxs = locSvcs.get(name);

            needDeploy = (ctxs == null) || (ctxs.size() != expNum);
        }

        if (needDeploy) {
            try {
                GridServiceAssignments old = srvcsAssigns.get(name);

                redeploy(old.name(), old.configuration(), assigns, old.cacheName(), old.affinityKey());
            }
            catch (Error | RuntimeException t) {
                errors.put(name, t);
            }
        }
    }

    /**
     * @param exchId Exchange id.
     * @param errors Deployment errors.
     */
    private void createAndSendSingleAssingmentsMessage(ServicesDeploymentExchangeId exchId,
        final Map<String, Throwable> errors) {
        ServicesSingleAssignmentsMessage msg = createSingleAssignmentsMessage(exchId, errors);

        ClusterNode crd = coordinator();

        try {
            ctx.io().sendToGridTopic(crd, TOPIC_SERVICES, msg, SERVICE_POOL);

            if (log.isDebugEnabled())
                log.debug("Send services single assignments message, msg=" + msg);
        }
        catch (IgniteCheckedException e) {
            if (log.isDebugEnabled() && X.hasCause(e, ClusterTopologyCheckedException.class))
                log.debug("Topology changed while message send: " + e.getMessage());

            log.error("Failed to send message over communication spi, msg=" + msg, e);
        }
    }

    /**
     * @param exchId Exchange id.
     * @param errors Deployment errors.
     * @return Services single assignments message.
     */
    private ServicesSingleAssignmentsMessage createSingleAssignmentsMessage(ServicesDeploymentExchangeId exchId,
        Map<String, Throwable> errors) {
        Map<String, Integer> locAssings = new HashMap<>();

        locSvcs.forEach((name, ctxs) -> {
            if (!ctxs.isEmpty())
                locAssings.put(name, ctxs.size());
        });

        ServicesSingleAssignmentsMessage msg = new ServicesSingleAssignmentsMessage(
            ctx.localNodeId(),
            exchId,
            locAssings
        );

        if (!errors.isEmpty()) {
            Map<String, byte[]> errorsBytes = new HashMap<>();

            errors.forEach((name, err) -> {
                try {
                    byte[] arr = U.marshal(ctx, err);

                    errorsBytes.put(name, arr);
                }
                catch (IgniteCheckedException e) {
                    log.error("Failed to marshal a deployment exception: " + err.getMessage() + ']', e);
                }
            });

            msg.errors(errorsBytes);
        }

        return msg;
    }

    /**
     * Discovery events listener.
     */
    private class DiscoveryListener implements DiscoveryEventListener {
        /** If local node coordinator or not. */
        private volatile boolean crd = false;

        /** {@inheritDoc} */
        @Override public void onEvent(final DiscoveryEvent evt, final DiscoCache discoCache) {
            if (!enterBusy())
                return;

            try {
                final boolean curCrd = isLocalNodeCoordinator();

                final boolean crdChanged = crd != curCrd;

                if (crdChanged && !ctx.clientNode()) {
                    if (crd)
                        exchangeMgr.stopProcessing();
                    else
                        exchangeMgr.startProcessing();

                    crd = curCrd;
                }

                if (evt instanceof DiscoveryCustomEvent) {
                    DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)evt).customMessage();

                    if (msg instanceof ServicesDeploymentRequestMessage || msg instanceof ServicesCancellationRequestMessage) {
                        if (ctx.clientNode())
                            return;

                        if (log.isDebugEnabled() && curCrd) {
                            log.debug("Received services change state request: [locId=" + ctx.localNodeId() +
                                ", sender=" + evt.eventNode().id() +
                                ", msg=" + msg + ']');
                        }

                        exchangeMgr.processEvent(evt, discoCache.version());
                    }
                    else if (msg instanceof ServicesAssignmentsRequestMessage) {
                        if (log.isDebugEnabled()) {
                            log.debug("Received services assignments request: [locId=" + ctx.localNodeId() +
                                ", sender=" + evt.eventNode().id() +
                                ", msg=" + msg + ']');
                        }

                        depExe.execute(new DepRunnable() {
                            @Override public void run0() {
                                onAssignmentsRequest((ServicesAssignmentsRequestMessage)msg);
                            }
                        });
                    }
                    else if (msg instanceof ServicesReassignmentsRequestMessage) {
                        final ServicesReassignmentsRequestMessage msg0 = (ServicesReassignmentsRequestMessage)msg;

                        if (log.isDebugEnabled()) {
                            log.debug("Received services reassignments message: [locId=" + ctx.localNodeId() +
                                ", sender=" + evt.eventNode().id() +
                                ", msg=" + msg0 + ']');
                        }

                        depExe.execute(new DepRunnable() {
                            @Override public void run0() {
                                onReassignmentsRequest(msg0);
                            }
                        });
                    }
                    else if (msg instanceof ServicesFullAssignmentsMessage) {
                        final ServicesFullAssignmentsMessage msg0 = (ServicesFullAssignmentsMessage)msg;

                        if (log.isDebugEnabled()) {
                            log.debug("Received services full assignments message: [locId=" + ctx.localNodeId() +
                                ", sender=" + evt.eventNode().id() +
                                ", msg=" + msg0 + ']');
                        }

                        depExe.execute(new DepRunnable() {
                            @Override public void run0() {
                                processFullAssignment(msg0);

                                if (!ctx.clientNode())
                                    exchangeMgr.onReceiveFullAssignmentsMessage(msg0);
                            }
                        });
                    }
                    else if (msg instanceof DynamicCacheChangeBatch) {
                        DynamicCacheChangeBatch msg0 = (DynamicCacheChangeBatch)msg;

                        Set<String> cachesToStop = new HashSet<>();
                        for (DynamicCacheChangeRequest req : msg0.requests()) {
                            if (req.stop())
                                cachesToStop.add(req.cacheName());
                        }

                        if (!cachesToStop.isEmpty()) {
                            if (srvcsAssigns.entrySet().stream().anyMatch(e -> cachesToStop.contains(e.getValue().cacheName())))
                                exchangeMgr.processEvent(evt, discoCache.version());
                        }
                    }
                    if (msg instanceof CacheAffinityChangeMessage) {
                        if (srvcsAssigns.entrySet().stream().anyMatch(e -> e.getValue().cacheName() != null))
                            exchangeMgr.processEvent(evt, discoCache.version());
                    }

                    return;
                }

                if (ctx.clientNode())
                    return;

                switch (evt.type()) {
                    case EVT_NODE_LEFT:
                    case EVT_NODE_FAILED:
                    case EVT_NODE_JOINED:
                        if (!srvcsAssigns.isEmpty())
                            exchangeMgr.processEvent(evt, discoCache.version());

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
    private class CommunicationListener implements GridMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
            if (!enterBusy())
                return;

            try {
                if (msg instanceof ServicesSingleAssignmentsMessage) {
                    if (log.isDebugEnabled()) {
                        log.debug("Received services single assignment message, locId=" + ctx.localNodeId() +
                            ", msg=" + msg + ']');
                    }

                    exchangeMgr.onReceiveSingleAssignmentsMessage((ServicesSingleAssignmentsMessage)msg);
                }
            }
            finally {
                leaveBusy();
            }
        }
    }

    /**
     * @param name Service name to undeploy.
     */
    private void undeploy(String name) {
        srvcName.set(name);

        Collection<ServiceContextImpl> ctxs;

        synchronized (locSvcs) {
            ctxs = locSvcs.remove(name);
        }

        if (ctxs != null) {
            synchronized (ctxs) {
                cancel(ctxs, ctxs.size());
            }
        }
    }

    /**
     *
     */
    private abstract class DepRunnable implements Runnable {
        /** {@inheritDoc} */
        @Override public void run() {
            if (!enterBusy())
                return;

            // Won't block ServiceProcessor stopping process.
            leaveBusy();

            srvcName.set(null);

            try {
                run0();
            }
            catch (Throwable t) {
                log.error("Error when executing service: " + srvcName.get(), t);

                if (t instanceof Error)
                    throw t;
            }
            finally {
                srvcName.set(null);
            }
        }

        /**
         * Abstract run method protected by busy lock.
         */
        public abstract void run0();
    }

    /**
     * @param msg Services full assignments message.
     */
    private void processFullAssignment(ServicesFullAssignmentsMessage msg) {
        try {
            Map<String, ServiceAssignmentsMap> fullAssigns = msg.assigns();

            Map<String, Collection<byte[]>> fullErrors = msg.errors();

            synchronized (mux) {
                fullErrors.forEach((name, errors) -> {
                    GridServiceDeploymentFuture fut = depFuts.remove(name);

                    if (fut != null)
                        processDeploymentErrors(fut, errors);
                });

                fullAssigns.forEach((name, assigns) -> {
                    GridServiceAssignments srvcAssigns = srvcsAssigns.get(name);

                    if (srvcAssigns != null) {
                        srvcAssigns.topologyVersion(assigns.topologyVersion());
                        srvcAssigns.assigns(assigns.assigns());

                        Integer expNum = srvcAssigns.assigns().get(ctx.localNodeId());

                        if (expNum == null || expNum == 0)
                            undeploy(name);
                        else {
                            Collection ctxs = locSvcs.get(name);

                            if ((ctxs == null && expNum > 0) || (ctxs != null && expNum != ctxs.size()))
                                redeploy(srvcAssigns);
                        }
                    }
                    else if (log.isDebugEnabled()) {
                        log.debug("Unexpected state: service assignments are contained in full map, " +
                            "but are not contained in the local storage.");
                    }

                    GridServiceDeploymentFuture fut = depFuts.remove(name);

                    if (fut != null) {
                        if (!srvcAssigns.configuration().equalsIgnoreNodeFilter(fut.configuration())) {
                            fut.onDone(new IgniteCheckedException("Failed to deploy service (service already exists with " +
                                "different configuration) [deployed=" + srvcAssigns.configuration() +
                                ", new=" + fut.configuration() + ']'));
                        }
                        else {
                            Collection<byte[]> errors = fullErrors.get(name);

                            if (errors == null)
                                fut.onDone();
                            else
                                processDeploymentErrors(fut, errors);
                        }
                    }
                });

                Set<String> srvcsNames = fullAssigns.keySet();

                Set<String> locSrvcsNames = new HashSet<>(locSvcs.keySet());

                for (String name : locSrvcsNames) {
                    if (!srvcsNames.contains(name))
                        undeploy(name);
                }

                srvcsAssigns.entrySet().removeIf(assign -> !srvcsNames.contains(assign.getKey()));

                undepFuts.entrySet().removeIf(e -> {
                    String svcName = e.getKey();
                    GridFutureAdapter<?> fut = e.getValue();

                    if (!srvcsNames.contains(svcName)) {
                        fut.onDone();

                        return true;
                    }

                    return false;
                });

                if (log.isDebugEnabled() && (!depFuts.isEmpty() || !undepFuts.isEmpty())) {
                    log.debug("Detected incomplete futures, after full map processing: " + fullAssigns);

                    if (!depFuts.isEmpty())
                        log.debug("Deployment futures: " + depFuts);

                    if (!undepFuts.isEmpty())
                        log.debug("Undeployment futures: " + undepFuts);
                }
            }
        }
        catch (Exception e) {
            log.error("Error occurred during processing of full assignments message, msg=" + msg, e);
        }
    }

    /**
     * @param fut Service deployment future.
     * @param errors Serialized errors.
     */
    private void processDeploymentErrors(GridServiceDeploymentFuture fut, Collection<byte[]> errors) {
        ServiceConfiguration srvcCfg = fut.configuration();

        ServiceDeploymentException ex = null;

        for (byte[] error : errors) {
            try {
                Throwable t = U.unmarshal(ctx, error, null);

                if (ex == null)
                    ex = new ServiceDeploymentException(t, Collections.singleton(srvcCfg));
                else
                    ex.addSuppressed(t);
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to unmarshal deployment exception.", e);
            }
        }

        log.error("Failed to deploy service, name=" + srvcCfg.getName(), ex);

        fut.onDone(ex);
    }

    /**
     * @return {@code true} if local node is clusters coordinator, otherwise {@code false}.
     */
    private boolean isLocalNodeCoordinator() {
        DiscoverySpi spi = ctx.discovery().getInjectedDiscoverySpi();

        if (spi instanceof TcpDiscoverySpi)
            return ((TcpDiscoverySpi)spi).isLocalNodeCoordinator();
        else {
            ClusterNode node = coordinator();

            return node != null && node.isLocal();
        }
    }

    /**
     * @return Coordinator node or {@code null} if there are no coordinator.
     */
    @Nullable private ClusterNode coordinator() {
        try {
            return U.oldest(ctx.discovery().aliveServerNodes(), null);
        }
        catch (Exception ignored) {
            return null;
        }
    }

    /**
     * Enters busy state.
     *
     * @return {@code true} if entered to busy state.
     */
    private boolean enterBusy() {
        GridSpinBusyLock busyLock = GridServiceProcessor.this.busyLock;

        return busyLock != null && busyLock.enterBusy();
    }

    /**
     * Leaves busy state.
     */
    private void leaveBusy() {
        GridSpinBusyLock busyLock = GridServiceProcessor.this.busyLock;

        if (busyLock != null)
            busyLock.leaveBusy();
    }

    /**
     * Initial data container to send on joined node.
     */
    private static class InitialServicesData implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Services assignments. */
        private ArrayList<GridServiceAssignments> assigns;

        /**
         * @param assigns Services assignments.
         */
        public InitialServicesData(ArrayList<GridServiceAssignments> assigns) {
            this.assigns = assigns;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(InitialServicesData.class, this);
        }
    }

    /**
     * @return Unmodifiable map of current services assignments.
     */
    public Map<String, GridServiceAssignments> assignments() {
        return Collections.unmodifiableMap(srvcsAssigns);
    }

    /**
     * @return Services assignments function.
     */
    public ServiceAssignmentsFunction assignmentsFunction() {
        return assignsFunc;
    }
}
