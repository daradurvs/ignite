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
import java.util.ArrayDeque;
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
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
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
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.thread.IgniteThreadFactory;
import org.apache.ignite.thread.OomExceptionHandler;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SERVICES_COMPATIBILITY_MODE;
import static org.apache.ignite.IgniteSystemProperties.getString;
import static org.apache.ignite.configuration.DeploymentMode.ISOLATED;
import static org.apache.ignite.configuration.DeploymentMode.PRIVATE;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.SERVICE_PROC;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SERVICES_COMPATIBILITY_MODE;

/**
 * Grid service processor.
 */
@SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter", "ConstantConditions"})
public class GridServiceProcessor extends GridProcessorAdapter implements IgniteChangeGlobalStateSupport {
    /** */
    private final Boolean srvcCompatibilitySysProp;

    /** Local service instances. */
    private final Map<IgniteUuid, Collection<ServiceContextImpl>> locSvcs = new HashMap<>();

    /** Deployment futures. */
    private final ConcurrentMap<IgniteUuid, GridServiceDeploymentFuture> depFuts = new ConcurrentHashMap<>();

    /** Deployment futures. */
    private final ConcurrentMap<IgniteUuid, GridFutureAdapter<?>> undepFuts = new ConcurrentHashMap<>();

    /** Uncaught exception handler for thread pools. */
    private final UncaughtExceptionHandler oomeHnd = new OomExceptionHandler(ctx);

    /** Thread factory. */
    private ThreadFactory threadFactory = new IgniteThreadFactory(ctx.igniteInstanceName(), "service",
        oomeHnd);

    /** Services topologies update mutex. */
    private final Object srvcsTopsUpdateMux = new Object();

    /** Contains all services information across the cluster. */
    private final ConcurrentMap<IgniteUuid, ServiceInfo> registeredSrvcs = new ConcurrentHashMap<>();

    /** Connection status lock. */
    private final ReadWriteLock connStatusLock = new ReentrantReadWriteLock();

    /** Services deployment exchange manager. */
    private ServicesDeploymentExchangeManager exchMgr = new ServicesDeploymentExchangeManager(ctx);

    /** Client disconnected flag. */
    private boolean disconnected;

    /** Local node's joining data. */
    private ServicesJoinNodeDiscoveryData locData;

    /**
     * @param ctx Kernal context.
     */
    public GridServiceProcessor(GridKernalContext ctx) {
        super(ctx);

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

        ServiceConfiguration[] cfgs = ctx.config().getServiceConfiguration();

        final ArrayList<ServiceInfo> staticSrvcsInfo = new ArrayList<>();

        if (cfgs != null) {
            // Skipped check of marshalling, because {@link GridMarshallerMappingProcessor} is not started at this point.
            PreparedConfigurations prepCfgs = prepareServiceConfigurations(Arrays.asList(cfgs),
                node -> !node.isClient(), false);

            if (prepCfgs.failedFuts != null) {
                for (GridServiceDeploymentFuture fut : prepCfgs.failedFuts) {
                    log.warning("Failed to validate static service configuration (won't be deployed), " +
                        "cfg=" + fut.configuration() + ", err=" + fut.result());
                }
            }

            for (ServiceConfiguration srvcCfg : prepCfgs.cfgs)
                staticSrvcsInfo.add(new ServiceInfo(ctx.localNodeId(), IgniteUuid.randomUuid(), srvcCfg));
        }

        locData = new ServicesJoinNodeDiscoveryData(staticSrvcsInfo);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        if (ctx.isDaemon())
            return;

        exchMgr.startProcessing();

        if (log.isDebugEnabled())
            log.debug("Started service processor.");
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        if (ctx.isDaemon())
            return;

        exchMgr.stopProcessing();

        Exception ex = new IgniteCheckedException("Operation has been cancelled (node is stopping).");

        onKernalStop(ex);

        if (log.isDebugEnabled())
            log.debug("Stopped service processor.");
    }

    /**
     * @param ex Error to cancel waiting futures.
     */
    private void onKernalStop(Exception ex) {
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

        cancelFutures(depFuts, ex);
        cancelFutures(undepFuts, ex);
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        if (dataBag.commonDataCollectedFor(SERVICE_PROC.ordinal()))
            return;

        ServicesCommonDiscoveryData clusterData = new ServicesCommonDiscoveryData(
            new ArrayList<>(registeredSrvcs.values()),
            new ArrayDeque<>(ctx.service().exchange().tasks())
        );

        dataBag.addGridCommonData(SERVICE_PROC.ordinal(), clusterData);
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        if (ctx.isDaemon() || data.commonData() == null)
            return;

        ServicesCommonDiscoveryData clusterData = (ServicesCommonDiscoveryData)data.commonData();

        if (disconnected)
            registeredSrvcs.clear();

        for (ServiceInfo desc : clusterData.registeredServices())
            registeredSrvcs.put(desc.serviceId(), desc);

        clusterData.exchangeQueue().forEach(t -> ctx.service().exchange().addTask(t.exchangeId(), t.customMessage()));
    }

    /** {@inheritDoc} */
    @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        assert locData != null;

        dataBag.addJoiningNodeData(SERVICE_PROC.ordinal(), locData);
    }

    /** {@inheritDoc} */
    @Override public void onJoiningNodeDataReceived(DiscoveryDataBag.JoiningNodeDiscoveryData data) {
        if (data.joiningNodeData() == null)
            return;

        ServicesJoinNodeDiscoveryData joinData = (ServicesJoinNodeDiscoveryData)data.joiningNodeData();

        for (ServiceInfo toStart : joinData.services()) {
            assert toStart.topologySnapshot().isEmpty();

            boolean exists = false;

            for (ServiceInfo desc : registeredSrvcs.values()) {
                if (desc.configuration().equalsIgnoreNodeFilter(toStart.configuration())) {
                    exists = true;

                    break;
                }
            }

            if (!exists)
                registeredSrvcs.put(toStart.serviceId(), toStart);
            else {
                log.warning("Ignore service configuration received from joining node : " +
                    "[nodeId=" + data.joiningNodeId() + ", cfgName=" + toStart.name() + "]. " +
                    "The same service configuration already registered.");
            }
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryDataExchangeType discoveryDataType() {
        return SERVICE_PROC;
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        if (log.isDebugEnabled())
            log.debug("DeActivate service processor [nodeId=" + ctx.localNodeId() +
                " topVer=" + ctx.discovery().topologyVersionEx() + " ]");

        Exception ex = new IgniteCheckedException("Operation has been cancelled (node is deactivating).");

        onKernalStop(ex);
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) {
        connStatusLock.writeLock().lock();

        try {
            disconnected = true;

            exchMgr.stopProcessing();

            exchMgr = new ServicesDeploymentExchangeManager(ctx);

            IgniteClientDisconnectedCheckedException err = new IgniteClientDisconnectedCheckedException(
                ctx.cluster().clientReconnectFuture(), "Client node disconnected, the operation's result is unknown.");

            cancelFutures(depFuts, err);
            cancelFutures(undepFuts, err);
        }
        finally {
            connStatusLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> onReconnected(boolean active) {
        connStatusLock.writeLock().lock();

        try {
            exchMgr.startProcessing();

            disconnected = false;

            return null;
        }
        finally {
            connStatusLock.writeLock().unlock();
        }
    }

    /**
     * @param futs Futs.
     * @param err Exception.
     */
    private void cancelFutures(ConcurrentMap<IgniteUuid, ? extends GridFutureAdapter<?>> futs, Exception err) {
        for (Map.Entry<IgniteUuid, ? extends GridFutureAdapter<?>> entry : futs.entrySet()) {
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
     * @param checkMarshalling {@code true} if it is necessary to check marshalling of {@link
     * ServiceConfiguration#getService()}, otherwise false.
     * @return Configurations to deploy.
     */
    private PreparedConfigurations prepareServiceConfigurations(Collection<ServiceConfiguration> cfgs,
        IgnitePredicate<ClusterNode> dfltNodeFilter, boolean checkMarshalling) {
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
                if (checkMarshalling) {
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
                else
                    cfgsCp.add(cfg);
            }

            if (err != null) {
                if (failedFuts == null)
                    failedFuts = new ArrayList<>();

                GridServiceDeploymentFuture fut = new GridServiceDeploymentFuture(cfg, null);

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
            return deployAll(cfgs,  ctx.cluster().get().forServers().predicate());
        else if (prj.predicate() == F.<ClusterNode>alwaysTrue())
            return deployAll(cfgs,  null);
        else
            // Deploy to predicate nodes by default.
            return deployAll(cfgs,  prj.predicate());
    }

    /**
     * @param cfgs Service configurations.
     * @param dfltNodeFilter Default NodeFilter.
     * @return Future for deployment.
     */
    private IgniteInternalFuture<?> deployAll(Collection<ServiceConfiguration> cfgs,
        @Nullable IgnitePredicate<ClusterNode> dfltNodeFilter) {
        assert cfgs != null;

        connStatusLock.readLock().lock();

        try {
            if (disconnected) {
                IgniteClientDisconnectedCheckedException err = new IgniteClientDisconnectedCheckedException(
                    ctx.cluster().clientReconnectFuture(), "Failed to deploy services, client node disconnected: " + cfgs);

                return new GridFinishedFuture<>(err);
            }

            PreparedConfigurations srvCfg = prepareServiceConfigurations(cfgs, dfltNodeFilter, true);

            List<ServiceConfiguration> cfgsCp = srvCfg.cfgs;

            List<GridServiceDeploymentFuture> failedFuts = srvCfg.failedFuts;

            GridServiceDeploymentCompoundFuture res = new GridServiceDeploymentCompoundFuture();

            if (!cfgsCp.isEmpty()) {
                cfgsCp.sort(Comparator.comparing(ServiceConfiguration::getName));

                while (true) {
                    try {
                        Collection<DynamicServiceChangeRequest> reqs = new ArrayList<>();

                        for (ServiceConfiguration cfg : cfgsCp) {
                            IgniteUuid srvcId = IgniteUuid.randomUuid();

                            GridServiceDeploymentFuture fut = new GridServiceDeploymentFuture(cfg, srvcId);

                            res.add(fut, true);

                            DynamicServiceChangeRequest req = DynamicServiceChangeRequest.deploymentRequest(srvcId, cfg);

                            reqs.add(req);

                            depFuts.put(srvcId, fut);
                        }

                        DynamicServicesChangeRequestBatchMessage msg = new DynamicServicesChangeRequestBatchMessage(reqs);

                        ctx.discovery().sendCustomEvent(msg);

                        if (log.isDebugEnabled())
                            log.debug("Services have been sent to deploy, req=" + msg);

                        break;
                    }
                    catch (IgniteException | IgniteCheckedException e) {
                        for (IgniteUuid id : res.servicesToRollback())
                            depFuts.remove(id).onDone(e);

                        if (X.hasCause(e, ClusterTopologyCheckedException.class)) {
                            res = new GridServiceDeploymentCompoundFuture();

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

            if (failedFuts != null) {
                for (GridServiceDeploymentFuture fut : failedFuts)
                    res.add(fut, false);
            }

            res.markInitialized();

            return res;
        }
        finally {
            connStatusLock.readLock().unlock();
        }
    }

    /**
     * @param name Service name.
     * @return Future.
     */
    public IgniteInternalFuture<?> cancel(String name) {
        return cancelAll(Collections.singleton(name));
    }

    /**
     * @return Future.
     */
    public IgniteInternalFuture<?> cancelAll() {
        return cancelAll(registeredSrvcs.keySet());
    }

    /**
     * @param svcNames Name of service to deploy.
     * @return Future.
     */
    public IgniteInternalFuture<?> cancelAll(Collection<String> svcNames) {
        Set<IgniteUuid> srvcsIds = new HashSet<>();

        for (String name : svcNames) {
            IgniteUuid srvcId = lookupId(name);

            if (srvcId != null)
                srvcsIds.add(srvcId);
            else if (log.isDebugEnabled())
                log.debug("Service id has not been found, name=" + name);
        }

        return cancelAll(srvcsIds);
    }

    /**
     * @param srvcsIds Services ids to cancel.
     * @return Future.
     */
    @SuppressWarnings("unchecked")
    private IgniteInternalFuture<?> cancelAll(Set<IgniteUuid> srvcsIds) {
        connStatusLock.readLock().lock();

        try {
            if (disconnected) {
                IgniteClientDisconnectedCheckedException err = new IgniteClientDisconnectedCheckedException(
                    ctx.cluster().clientReconnectFuture(), "Failed to undeploy services, client node disconnected.");

                return new GridFinishedFuture<>(err);
            }

            GridCompoundFuture res = new GridCompoundFuture<>();

            if (!srvcsIds.isEmpty()) {
                while (true) {
                    Set<IgniteUuid> toRollback = new HashSet<>();

                    List<DynamicServiceChangeRequest> reqs = new ArrayList<>();

                    try {
                        for (IgniteUuid srvcId : srvcsIds) {
                            ServiceDescriptor desc = registeredSrvcs.get(srvcId);

                            try {
                                ctx.security().authorize(desc.name(), SecurityPermission.SERVICE_CANCEL, null);
                            }
                            catch (SecurityException e) {
                                res.add(new GridFinishedFuture<>(e));

                                continue;
                            }

                            GridFutureAdapter<?> fut = new GridFutureAdapter<>();

                            GridFutureAdapter<?> old = undepFuts.putIfAbsent(srvcId, fut);

                            if (old != null) {
                                res.add(old);

                                continue;
                            }

                            res.add(fut);

                            if (!registeredSrvcs.containsKey(srvcId)) {
                                fut.onDone();

                                continue;
                            }

                            toRollback.add(srvcId);

                            DynamicServiceChangeRequest req = DynamicServiceChangeRequest.undeploymentRequest(srvcId);

                            reqs.add(req);
                        }

                        if (!reqs.isEmpty()) {
                            DynamicServicesChangeRequestBatchMessage msg = new DynamicServicesChangeRequestBatchMessage(reqs);

                            ctx.discovery().sendCustomEvent(msg);

                            if (log.isDebugEnabled())
                                log.debug("Services have been sent to cancel, msg=" + msg);
                        }

                        break;
                    }
                    catch (IgniteException | IgniteCheckedException e) {
                        for (IgniteUuid id : toRollback)
                            undepFuts.remove(id).onDone(e);

                        if (X.hasCause(e, ClusterTopologyCheckedException.class)) {
                            res = new GridCompoundFuture<>();

                            if (log.isDebugEnabled())
                                log.debug("Topology changed while cancelling services (will retry): " + e.getMessage());
                        }
                        else {
                            U.error(log, "Failed to undeploy services: " + srvcsIds, e);

                            res.onDone(e);

                            return res;
                        }
                    }
                }
            }

            res.markInitialized();

            return res;
        }
        finally {
            connStatusLock.readLock().unlock();
        }
    }

    /**
     * @param name Service name.
     * @param timeout If greater than 0 limits task execution time. Cannot be negative.
     * @return Service topology.
     * @throws IgniteCheckedException On error.
     */
    public Map<UUID, Integer> serviceTopology(String name, long timeout) throws IgniteCheckedException {
        assert timeout >= 0;

        IgniteUuid id = lookupId(name);

        if (id == null) {
            if (log.isDebugEnabled()) {
                log.debug("Requested service topology have not been found: [srvcName=" + name +
                    ", locId=" + ctx.localNodeId() + ", client=" + ctx.clientNode() + ']');
            }

            return null;
        }

        ServiceDescriptor desc = registeredSrvcs.get(id);

        Map<UUID, Integer> dep = desc != null ? desc.topologySnapshot() : null;

        if ((dep == null || dep.isEmpty()) && timeout > 0) {
            synchronized (srvcsTopsUpdateMux) {
                try {
                    srvcsTopsUpdateMux.wait(timeout);
                }
                catch (InterruptedException e) {
                    throw new IgniteInterruptedCheckedException(e);
                }

                desc = registeredSrvcs.get(id);

                return desc != null ? desc.topologySnapshot() : null;
            }
        }

        return dep;
    }

    /**
     * @return Collection of deployed service descriptors.
     */
    public Collection<ServiceDescriptor> serviceDescriptors() {
        Collection<ServiceDescriptor> descs = new ArrayList<>();

        registeredSrvcs.forEach((srvcId, desc) -> {
            Map<UUID, Integer> top = desc.topologySnapshot();

            if (top != null && !top.isEmpty())
                descs.add(desc);
        });

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

        IgniteUuid srvcId = lookupId(name);

        if (srvcId == null)
            return null;

        synchronized (locSvcs) {
            ctxs = locSvcs.get(srvcId);
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

        IgniteUuid srvcId = lookupId(name);

        if (srvcId == null)
            return null;

        synchronized (locSvcs) {
            ctxs = locSvcs.get(srvcId);
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

        IgniteUuid srvcId = lookupId(name);

        if (srvcId == null)
            return null;

        synchronized (locSvcs) {
            ctxs = locSvcs.get(srvcId);
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
     * @param srvcId Service id.
     * @param cfg Service configuration.
     * @param topVer Topology version.
     * @throws IgniteCheckedException If failed.
     */
    public Map<UUID, Integer> reassign(IgniteUuid srvcId, ServiceConfiguration cfg,
        AffinityTopologyVersion topVer) throws IgniteCheckedException {
        Object nodeFilter = cfg.getNodeFilter();

        if (nodeFilter != null)
            ctx.resource().injectGeneric(nodeFilter);

        int totalCnt = cfg.getTotalCount();
        int maxPerNodeCnt = cfg.getMaxPerNodeCount();
        String cacheName = cfg.getCacheName();
        Object affKey = cfg.getAffinityKey();

        while (true) {
            Collection<ClusterNode> nodes;

            if (affKey == null) {
                nodes = ctx.discovery().nodes(topVer);

                if (cfg.getNodeFilter() != null) {
                    Collection<ClusterNode> nodes0 = new ArrayList<>();

                    for (ClusterNode node : nodes) {
                        if (cfg.getNodeFilter().apply(node))
                            nodes0.add(node);
                    }

                    nodes = nodes0;
                }
            }
            else
                nodes = null;

            try {
                String name = cfg.getName();

                IgniteUuid id = lookupId(name);

                Map<UUID, Integer> oldTop = id != null ? registeredSrvcs.get(id).topologySnapshot() : null;

                Map<UUID, Integer> cnts = new HashMap<>();

                if (affKey != null && cacheName != null) {
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

                            Random rnd = new Random(srvcId.localId());

                            if (oldTop != null) {
                                Collection<UUID> used = new HashSet<>();

                                // Avoid redundant moving of services.
                                for (Map.Entry<UUID, Integer> e : oldTop.entrySet()) {
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
                                    Collections.shuffle(entries, rnd);

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
                                Collections.shuffle(entries, rnd);

                                for (Map.Entry<UUID, Integer> e : entries) {
                                    e.setValue(e.getValue() + 1);

                                    if (--remainder == 0)
                                        break;
                                }
                            }
                        }
                    }
                }

                return cnts;
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
     * @param srvcId Service id.
     * @param cfg Service configuration.
     * @param top Service topology.
     */
    protected void redeploy(IgniteUuid srvcId, ServiceConfiguration cfg, Map<UUID, Integer> top) {
        String name = cfg.getName();
        String cacheName = cfg.getCacheName();
        Object affKey = cfg.getAffinityKey();

        Integer assignCnt = top.get(ctx.localNodeId());

        if (assignCnt == null)
            assignCnt = 0;

        Collection<ServiceContextImpl> ctxs;

        synchronized (locSvcs) {
            ctxs = locSvcs.get(srvcId);

            if (ctxs == null)
                locSvcs.put(srvcId, ctxs = new ArrayList<>());
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
     * @param srvcId Service id.
     */
    protected void undeploy(IgniteUuid srvcId) {
        registeredSrvcs.remove(srvcId);

        Collection<ServiceContextImpl> ctxs;

        synchronized (locSvcs) {
            ctxs = locSvcs.remove(srvcId);
        }

        if (ctxs != null) {
            synchronized (ctxs) {
                cancel(ctxs, ctxs.size());
            }
        }
    }

    /**
     * Handles services full map message.
     *
     * @param msg Services full map message.
     */
    protected void processFullMap(ServicesFullMapMessage msg) {
        connStatusLock.readLock().lock();

        try {
            if (disconnected)
                return;

            final Collection<ServiceFullDeploymentsResults> results = msg.results();

            final Map<IgniteUuid, HashMap<UUID, Integer>> fullTops = new HashMap<>();
            final Map<IgniteUuid, Collection<byte[]>> fullErrors = new HashMap<>();

            for (ServiceFullDeploymentsResults depRes : results) {
                final IgniteUuid srvcId = depRes.serviceId();
                final Map<UUID, ServiceSingleDeploymentsResults> deps = depRes.results();

                final HashMap<UUID, Integer> top = new HashMap<>();
                final Collection<byte[]> errors = new ArrayList<>();

                deps.forEach((nodeId, res) -> {
                    int cnt = res.count();

                    if (cnt > 0)
                        top.put(nodeId, cnt);

                    if (!res.errors().isEmpty())
                        errors.addAll(res.errors());
                });

                if (!errors.isEmpty())
                    fullErrors.computeIfAbsent(srvcId, e -> new ArrayList<>()).addAll(errors);

                if (top.isEmpty()) {
                    undeploy(srvcId);

                    continue;
                }

                ServiceInfo srvcDesc = registeredSrvcs.get(srvcId);

                assert srvcDesc != null : "Service descriptor has not been found to undeploy exceed instances, " +
                    "client=" + ctx.clientNode() + ", disconnected=" + disconnected;

                fullTops.put(srvcId, top);

                Integer expCnt = top.getOrDefault(ctx.localNodeId(), 0);

                Collection ctxs = locSvcs.get(srvcId);

                if (ctxs != null && expCnt < ctxs.size()) { // Undeploy exceed instances
                    ServiceConfiguration cfg = srvcDesc.configuration();

                    redeploy(srvcId, cfg, top);
                }
            }

            final Set<IgniteUuid> srvcsIds = fullTops.keySet();

            synchronized (srvcsTopsUpdateMux) {
                fullTops.forEach((srvcId, top) -> {
                    ServiceInfo desc = registeredSrvcs.get(srvcId);

                    assert desc != null : "Service descriptor has not been found to update deployment topology, " +
                        "client=" + ctx.clientNode() + ", disconnected=" + disconnected;

                    desc.topologySnapshot(top);
                });

                srvcsTopsUpdateMux.notifyAll();
            }

            depFuts.entrySet().removeIf(entry -> {
                IgniteUuid srvcId = entry.getKey();
                GridServiceDeploymentFuture fut = entry.getValue();

                Collection<byte[]> errors = fullErrors.get(srvcId);

                ServiceConfiguration cfg = fut.configuration();

                if (errors != null) {
                    ServiceDeploymentException ex = null;

                    for (byte[] error : errors) {
                        try {
                            Throwable t = U.unmarshal(ctx, error, null);

                            if (ex == null)
                                ex = new ServiceDeploymentException(t, Collections.singleton(cfg));
                            else
                                ex.addSuppressed(t);
                        }
                        catch (IgniteCheckedException e) {
                            log.error("Failed to unmarshal deployment exception.", e);
                        }
                    }

                    log.error("Failed to deploy service, name=" + cfg.getName(), ex);

                    fut.onDone(ex);

                    return true;
                }
                else {
                    for (ServiceInfo dep : registeredSrvcs.values()) {
                        if (dep.configuration().equalsIgnoreNodeFilter(cfg)) {
                            fut.onDone();

                            return true;
                        }
                    }
                }

                return false;
            });

            undepFuts.entrySet().removeIf(entry -> {
                IgniteUuid srvcId = entry.getKey();
                GridFutureAdapter<?> fut = entry.getValue();

                if (!srvcsIds.contains(srvcId)) {
                    fut.onDone();

                    return true;
                }

                return false;
            });

            if (log.isDebugEnabled() && (!depFuts.isEmpty() || !undepFuts.isEmpty())) {
                log.debug("Detected incomplete futures, after full map processing" +
                    ", services topologies=" + fullTops +
                    (!depFuts.isEmpty() ? ", depFuts=" + depFuts : "") +
                    (!undepFuts.isEmpty() ? ", undepFuts=" + undepFuts.keySet() : "")
                );
            }
        }
        catch (Exception e) {
            log.error("Error occurred while processing services' full map message." +
                " [locNode=" + ctx.localNodeId() + ", msg=" + msg, e);
        }
        finally {
            connStatusLock.readLock().unlock();
        }
    }

    /**
     * @param name Service name;
     * @return @return Service's id if exists, otherwise {@code null};
     */
    @Nullable private IgniteUuid lookupId(String name) {
        for (ServiceInfo desc : registeredSrvcs.values()) {
            if (desc.name().equals(name))
                return desc.serviceId();
        }

        return null;
    }

    /**
     * @param srvcId Service id.
     * @return Count of locally deployed service with given id.
     */
    protected int localInstancesCount(IgniteUuid srvcId) {
        Collection<ServiceContextImpl> ctxs = locSvcs.get(srvcId);

        return ctxs != null ? ctxs.size() : 0;
    }

    /**
     * @return Map with counts of all locally deployed services.
     */
    @NotNull protected Map<IgniteUuid, Integer> localInstancesCount() {
        Map<IgniteUuid, Integer> locTop = new HashMap<>();

        locSvcs.forEach((srvcId, ctxs) -> {
            locTop.put(srvcId, ctxs.size());
        });

        return locTop;
    }

    /**
     * @return Ids of affinity services deployed across the cluster.
     */
    @NotNull protected Set<IgniteUuid> affinityServices() {
        Set<IgniteUuid> srvcs = new HashSet<>();

        registeredSrvcs.values().forEach(desc -> {
            if (desc.configuration().getCacheName() != null)
                srvcs.add(desc.serviceId());
        });

        return srvcs;
    }

    /**
     * @param cacheName Cache name.
     * @return Id of affinity service with given cache name. Possibly {@code null} if not found.
     */
    @Nullable protected IgniteUuid affinityService(String cacheName) {
        for (ServiceInfo desc : registeredSrvcs.values()) {
            if (desc.cacheName().equals(cacheName))
                return desc.serviceId();
        }

        return null;
    }

    /**
     * @param srvcId Service id.
     * @return Service configuration of service deployed with given id. Possibly {@code null} if not found.
     */
    @Nullable protected ServiceConfiguration serviceConfiguration(IgniteUuid srvcId) {
        ServiceInfo desc = registeredSrvcs.get(srvcId);

        return desc != null ? desc.configuration() : null;
    }

    /**
     * @return Ids of all deployed services.
     */
    protected Set<IgniteUuid> deployedServicesIds() {
        Set<IgniteUuid> ids = new HashSet<>();

        registeredSrvcs.forEach((srvcId, desc) -> {
            if (!desc.topologySnapshot().isEmpty())
                ids.add(srvcId);
        });

        return ids;
    }

    /**
     * @return Ids of all registered services.
     */
    protected Set<IgniteUuid> registeredServicesIds() {
        return new HashSet<>(registeredSrvcs.keySet());
    }

    /**
     * @param srvcId Service id.
     * @return {@link ServiceInfo} instance of deployed service with given id. Possibly {@code null} if not found.
     */
    @Nullable protected ServiceInfo serviceInfo(IgniteUuid srvcId) {
        return registeredSrvcs.get(srvcId);
    }

    /**
     * @param name Service name.
     * @return {@link ServiceInfo} instance of deployed service with given id. Possibly {@code null} if not found.
     */
    @Nullable protected ServiceInfo serviceInfo(String name) {
        IgniteUuid srvcId = lookupId(name);

        return srvcId != null ? serviceInfo(srvcId) : null;
    }

    /**
     * @param srvcId Service id.
     * @param desc {@link ServiceInfo} instance.
     */
    protected void putIfAbsentServiceInfo(IgniteUuid srvcId, ServiceInfo desc) {
        registeredSrvcs.putIfAbsent(srvcId, desc);
    }

    /**
     * Gets services received to deploy from node with given id on joining.
     *
     * @param nodeId Joined node id.
     * @return List of services to deploy received on node joining with given id.
     */
    @NotNull protected Set<IgniteUuid> servicesReceivedFromJoin(UUID nodeId) {
        Set<IgniteUuid> ids = new HashSet<>();

        for (ServiceInfo desc : registeredSrvcs.values()) {
            if (desc.originNodeId().equals(nodeId))
                ids.add(desc.serviceId());
        }

        return ids;
    }

    /**
     * Special handler for local join events for which the regular events are not generated.
     * <p/>
     * Local join event is expected on joining to topology or client reconnect.
     *
     * @param evt Discovery event.
     * @param discoCache Discovery cache.
     */
    public void onLocalJoin(DiscoveryEvent evt, DiscoCache discoCache) {
        assert ctx.localNodeId().equals(evt.eventNode().id());
        assert evt.type() == EVT_NODE_JOINED;

        if (!evt.eventNode().isClient()) {
            boolean first;

            DiscoverySpi spi = ctx.discovery().getInjectedDiscoverySpi();

            if ((spi instanceof TcpDiscoverySpi))
                first = ((TcpDiscoverySpi)spi).isLocalNodeCoordinator();
            else
                first = F.eq(ctx.localNodeId(), U.oldest(ctx.discovery().aliveServerNodes(), null));

            // First node start, {@link #onGridDataReceived(DiscoveryDataBag.GridDiscoveryData)} has not been called
            if (first)
                locData.services().forEach(desc -> registeredSrvcs.put(desc.serviceId(), desc));
        }

        exchMgr.onLocalEvent(evt, discoCache);
    }

    /**
     * @return Services deployment exchange manager.
     */
    public ServicesDeploymentExchangeManager exchange() {
        return exchMgr;
    }
}
