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
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
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
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
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

    /** Discovery events to listen. */
    private static final int[] EVTS = {EVT_NODE_JOINED, EVT_NODE_LEFT, EVT_NODE_FAILED, EVT_DISCOVERY_CUSTOM_EVT};

    /** Local service instances. */
    private final Map<String, Collection<ServiceContextImpl>> locSvcs = new HashMap<>();

    /** Deployment futures. */
    private final ConcurrentMap<String, GridServiceDeploymentFuture> depFuts = new ConcurrentHashMap<>();

    /** Deployment futures. */
    private final ConcurrentMap<String, GridServiceUndeploymentFuture> undepFuts = new ConcurrentHashMap<>();

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
    private DiscoveryEventListener discoLsnr = new DiscoveryListener();

    /** Services messages communication listener. */
    private final CommunicationListener commLsnr = new CommunicationListener();

    /** Contains all services assignments, not only locally deployed. */
    private final Map<String, GridServiceAssignments> srvcsAssigns = new ConcurrentHashMap<>();

    /** Services deployment manager. */
    private final ServicesDeploymentExchangeManager exchangeMgr = new ServicesDeploymentExchangeManager(ctx);

    /** Services assignments function. */
    private final ServicesAssignmentsFunction assignsFunc = new ServicesAssignmentsFunctionImpl(ctx, srvcsAssigns);

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
        ctx.event().addDiscoveryEventListener(discoLsnr, EVTS);

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

        exchangeMgr.stopProcessing();

        if (!ctx.clientNode())
            ctx.event().removeDiscoveryEventListener(discoLsnr);

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

        srvcsAssigns.clear();

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
                                res.add(old, false); // Has been sent to deploy earlier.

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
                        ServicesDeploymentRequestMessage req = new ServicesDeploymentRequestMessage(
                            ctx.localNodeId(), cfgsToDeploy);

                        ctx.discovery().sendCustomEvent(req);

                        if (log.isDebugEnabled()) {
                            log.debug("Services sent to deploy: " +
                                "[locId:" + ctx.localNodeId() +
                                ", client:=" + ctx.clientNode() +
                                ", cfgs=" + cfgsToDeploy + ']');
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
                res.add(fut);
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
     * @param svcNames Names of services to cancel.
     * @return Future.
     */
    @SuppressWarnings("unchecked")
    public IgniteInternalFuture<?> cancelAll(Collection<String> svcNames) {
        GridCompoundFuture res;

        while (true) {
            res = new GridCompoundFuture<>();

            synchronized (mux) {
                List<String> toRollback = new ArrayList<>();

                List<String> svcsNamesToCancel = new ArrayList<>(svcNames.size());

                try {
                    for (String name : svcNames) {
                        try {
                            ctx.security().authorize(name, SecurityPermission.SERVICE_CANCEL, null);
                        }
                        catch (SecurityException e) {
                            res.add(new GridFinishedFuture<>(e));

                            continue;
                        }

                        GridServiceUndeploymentFuture fut = new GridServiceUndeploymentFuture(name);

                        GridServiceUndeploymentFuture old = undepFuts.putIfAbsent(name, fut);

                        if (old != null) { // Has been sent to undeploy earlier.
                            res.add(old);

                            continue;
                        }

                        res.add(fut);

                        if (!srvcsAssigns.containsKey(name)) {
                            fut.onDone();

                            continue;
                        }

                        toRollback.add(name);

                        svcsNamesToCancel.add(name);
                    }

                    if (!svcsNamesToCancel.isEmpty()) {
                        ServicesCancellationRequestMessage msg = new ServicesCancellationRequestMessage(
                            ctx.localNodeId(), svcsNamesToCancel);

                        ctx.discovery().sendCustomEvent(msg);

                        if (log.isDebugEnabled()) {
                            log.debug("Services sent to cancel: " +
                                "[locId:" + ctx.localNodeId() +
                                ", client:=" + ctx.clientNode() +
                                ", names=" + svcsNamesToCancel + ']');
                        }
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
                        U.error(log, "Failed to undeploy services: " + svcNames, e);

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
                    log.debug("Service assignment was not found : [" + name +
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
     * @param msg Service assignments message.
     */
    private void onAssignmentsRequest(ServicesAssignmentsRequestMessage msg) {
        try {
            Collection<GridServiceAssignments> assigns = msg.assignments();

            Map<String, byte[]> errors = new HashMap<>();

            Map<String, Integer> locAssings;

            synchronized (mux) {
                if (!ctx.clientNode()) {
                    locAssings = new HashMap<>();

                    for (GridServiceAssignments assign : assigns) {
                        String svcName = assign.name();

                        srvcsAssigns.putIfAbsent(svcName, assign);

                        Integer expNum = assign.assigns().get(ctx.localNodeId());

                        boolean needRedeploy = false;

                        if (expNum != null && expNum > 0) {
                            Collection<ServiceContextImpl> ctxs = locSvcs.get(svcName);

                            needRedeploy = (ctxs == null) || (ctxs.size() < expNum);
                        }

                        if (needRedeploy) {
                            try {
                                redeploy(assign);
                            }
                            catch (Error | RuntimeException th) {
                                try {
                                    byte[] arr = U.marshal(ctx, th);

                                    errors.put(assign.name(), arr);
                                }
                                catch (IgniteCheckedException e) {
                                    log.error("Failed to marshal a deployment exception: " + th.getMessage() + ']', e);
                                }
                            }
                        }
                    }

                    locSvcs.forEach((name, ctxs) -> {
                        if (!ctxs.isEmpty())
                            locAssings.put(name, ctxs.size());
                    });
                }
                else {
                    assigns.forEach(assign -> srvcsAssigns.putIfAbsent(assign.name(), assign));

                    locAssings = Collections.emptyMap();
                }
            }

            ServicesSingleAssignmentsMessage locAssignsMsg = new ServicesSingleAssignmentsMessage(
                ctx.localNodeId(),
                ctx.clientNode(),
                msg.exchangeId()
            );

            locAssignsMsg.assigns(locAssings);

            if (!errors.isEmpty())
                locAssignsMsg.errors(errors);

            ClusterNode crd = coordinator(ctx.discovery().topologyVersionEx());

            sendServiceMessage(crd, locAssignsMsg);
        }
        catch (Exception e) {
            log.error("Exception in #onAssignmentsRequest", e);
        }
    }

    /**
     * Discovery events listener.
     */
    private class DiscoveryListener implements DiscoveryEventListener {
        /** */
        private volatile boolean crd = false;

        /** {@inheritDoc} */
        @Override public void onEvent(final DiscoveryEvent evt, final DiscoCache discoCache) {
            if (!enterBusy())
                return;

            try {
                final boolean curCrd = isLocalNodeCoordinator(discoCache.version());

                final boolean crdChanged = crd != curCrd;

                if (crdChanged) {
                    if (crd)
                        exchangeMgr.stopProcessing();
                    else
                        exchangeMgr.startProcessing();

                    crd = curCrd;
                }

                if (evt instanceof DiscoveryCustomEvent) {
                    DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)evt).customMessage();

                    if (msg instanceof ServicesDeploymentRequestMessage || msg instanceof ServicesCancellationRequestMessage) {
                        if (!curCrd)
                            return;

                        ServicesDeploymentExchangeFuture fut = new ServicesDeploymentExchangeFuture(
                            srvcsAssigns, assignsFunc, ctx, evt);

                        exchangeMgr.onEvent(fut);
                    }
                    else if (msg instanceof ServicesAssignmentsRequestMessage) {
                        depExe.execute(() -> {
                            onAssignmentsRequest((ServicesAssignmentsRequestMessage)msg);
                        });
                    }
                    else if (msg instanceof ServicesFullAssignmentsMessage) {
                        if (log.isDebugEnabled())
                            log.debug("[TopLsnr: " +
                                " sender: " + evt.eventNode().id() +
                                " assigns: " + ((ServicesFullAssignmentsMessage)msg).assigns());

                        final ServicesFullAssignmentsMessage msg0 = (ServicesFullAssignmentsMessage)msg;

                        depExe.execute(() -> {
                            processFullAssignment(msg0);

                            exchangeMgr.onReceiveFullMessage(msg0);
                        });
                    }

                    return;
                }

                if (!curCrd && !evt.eventNode().isLocal())
                    return;

                switch (evt.type()) {
                    case EVT_NODE_LEFT:
                    case EVT_NODE_FAILED:
                        exchangeMgr.onNodeLeft(evt.eventNode().id());

                    case EVT_NODE_JOINED:
                        if (!srvcsAssigns.isEmpty()) {
                            ServicesDeploymentExchangeFuture fut = new ServicesDeploymentExchangeFuture(
                                srvcsAssigns, assignsFunc, ctx, evt);

                            exchangeMgr.onEvent(fut);
                        }

                        break;

                    default:
                        if (log.isDebugEnabled())
                            log.debug("Unexpected event, evt=" + evt);

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
                        log.debug("Received " + msg.getClass().getSimpleName() +
                            "; locId=" + ctx.localNodeId() +
                            "; client" + ctx.clientNode() +
                            "; sender=" + nodeId +
                            "; msg=" + msg + ']');
                    }

                    exchangeMgr.onReceiveSingleMessage((ServicesSingleAssignmentsMessage)msg);
                }

                // TODO: handle all service GridServiceAssignments
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
            Map<String, ServiceAssignmentsMap> fullAssignsMap = msg.assigns();

            Map<String, Collection<byte[]>> fullErrors = msg.errors();

            synchronized (mux) {
                fullAssignsMap.forEach((name, svcAssignsMap) -> {
                    GridServiceAssignments svsAssign = srvcsAssigns.get(name);

                    if (svsAssign != null) {
                        svsAssign.assigns(svcAssignsMap.assigns());

                        if (!ctx.clientNode()) {
                            Integer expNum = svsAssign.assigns().get(ctx.localNodeId());

                            if (expNum == null || expNum == 0)
                                undeploy(name);
                            else {
                                Collection ctxs = locSvcs.get(name);

                                if (ctxs != null && expNum < ctxs.size())
                                    redeploy(svsAssign);
                            }
                        }
                    }
                    else if (log.isDebugEnabled()) {
                        log.debug("Unexpected state: service assignments are contained in full map, " +
                            "but are not contained in the local storage.");
                    }

                    GridServiceDeploymentFuture fut = depFuts.remove(name);

                    if (fut != null) {
                        Collection<byte[]> errors = fullErrors.get(name);

                        if (errors == null)
                            fut.onDone();
                        else
                            processDeploymentErrors(fut, errors);
                    }
                });

                fullErrors.forEach((name, errors) -> {
                    GridServiceDeploymentFuture fut = depFuts.remove(name);

                    if (fut != null)
                        processDeploymentErrors(fut, errors);
                });

                Set<String> svcsNames = fullAssignsMap.keySet();

                Set<String> locSvcsNames = new HashSet<>(locSvcs.keySet());

                for (String svcName : locSvcsNames) {
                    if (!svcsNames.contains(svcName))
                        undeploy(svcName);
                }

                srvcsAssigns.entrySet().removeIf(assign -> !svcsNames.contains(assign.getKey()));

                undepFuts.entrySet().removeIf(e -> {
                    String svcName = e.getKey();
                    GridServiceUndeploymentFuture fut = e.getValue();

                    if (!svcsNames.contains(svcName)) {
                        fut.onDone();

                        return true;
                    }

                    return false;
                });

                if (log.isDebugEnabled() && (!depFuts.isEmpty() || !undepFuts.isEmpty())) {
                    log.debug("Detected incomplete futures, after full map processing: " + fullAssignsMap);

                    if (!depFuts.isEmpty())
                        log.debug("Deployment futures: " + depFuts);

                    if (!undepFuts.isEmpty())
                        log.debug("Undeployment futures: " + undepFuts);
                }
            }
        }
        catch (Exception e) {
            log.error("Exception in #processFullAssignment", e);
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
     * Sends message to node over communication spi.
     *
     * @param dest Destination node.
     * @param msg Message to send.
     */
    private void sendServiceMessage(ClusterNode dest, Message msg) {
        sendServiceMessage(dest.id(), msg);
    }

    /**
     * Sends message to node over communication spi.
     *
     * @param nodeId Destination node id.
     * @param msg Message to send.
     */
    private void sendServiceMessage(UUID nodeId, Message msg) {
        try {
            if (log.isDebugEnabled()) {
                log.debug("Send message: [locId=" + ctx.localNodeId() +
                    "; client=" + ctx.clientNode() +
                    "; destId=" + nodeId +
                    "; msg=" + msg + ']');
            }

            ctx.io().sendToGridTopic(nodeId, TOPIC_SERVICES, msg, SERVICE_POOL);
        }
        catch (IgniteCheckedException e) {
            if (log.isDebugEnabled() && X.hasCause(e, ClusterTopologyCheckedException.class))
                log.debug("Topology changed while message send: " + e.getMessage());

            log.error("Failed to send message over communication spi: [locId=" + ctx.localNodeId() +
                "; client=" + ctx.clientNode() +
                "; destId=" + nodeId +
                "; msg=" + msg + ']', e);
        }
    }

    /**
     * @return {@code true} if local node is clusters coordinator, otherwise {@code false}.
     */
    private boolean isLocalNodeCoordinator(AffinityTopologyVersion topVer) {
        if (ctx.clientNode())
            return false;

        ClusterNode crd = coordinator(topVer);

        return crd != null && crd.isLocal();
    }

    /**
     * @return Cluster coordinator, may be {@code null} in case of empty topology.
     */
    @Nullable private ClusterNode coordinator(AffinityTopologyVersion topVer) {
        return U.oldest(ctx.discovery().serverNodes(topVer), null);
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
}
