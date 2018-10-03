/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.service;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.SERVICE_PROC;

/**
 * Manages cluster services information on discovery exchange at node joining process.
 */
class ClusterServicesInfo {
    /** Mutex. */
    private final Object changeInfoMux = new Object();

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Local node's joining data. */
    private ServicesJoinNodeDiscoveryData locJoiningData;

    /** Services info received on node joining. */
    private List<ServiceInfo> srvcsToStart;

    /**
     * @param ctx Kernal context.
     */
    protected ClusterServicesInfo(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /**
     * @param locJoiningData Local node's joining data.
     */
    protected void onStart(@NotNull ServicesJoinNodeDiscoveryData locJoiningData) {
        this.locJoiningData = locJoiningData;
    }

    /**
     * @param data Joining node data.
     */
    protected void onJoiningNodeDataReceived(DiscoveryDataBag.JoiningNodeDiscoveryData data) {
        if (data.joiningNodeData() == null)
            return;

        assert srvcsToStart != null;

        ServicesJoinNodeDiscoveryData joinData = (ServicesJoinNodeDiscoveryData)data.joiningNodeData();

        synchronized (changeInfoMux) {
            for (ServiceInfo srvcToStart : joinData.services()) {
                boolean exists = false;

                for (ServiceInfo srvcInfo : srvcsToStart) {
                    if (srvcInfo.configuration().equalsIgnoreNodeFilter(srvcToStart.configuration())) {
                        exists = true;

                        break;
                    }
                }

                if (!exists)
                    srvcsToStart.add(srvcToStart);
            }
        }
    }

    /**
     * @param dataBag Discovery data bag to fill local join data.
     */
    protected void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        assert locJoiningData != null;

        dataBag.addJoiningNodeData(SERVICE_PROC.ordinal(), locJoiningData);
    }

    /**
     * @param dataBag Discovery data bag to fill.
     */
    protected void collectGridNodeData(DiscoveryDataBag dataBag) {
        if (dataBag.commonDataCollectedFor(SERVICE_PROC.ordinal()))
            return;

        ServicesCommonDiscoveryData initData;

        synchronized (changeInfoMux) {
            initData = new ServicesCommonDiscoveryData(
                ctx.service().servicesInfo(),
                new ArrayList<>(srvcsToStart),
                new ArrayDeque<>(ctx.service().exchange().tasks())
            );
        }

        dataBag.addGridCommonData(SERVICE_PROC.ordinal(), initData);
    }

    /**
     * @param data Cluster discovery data bag.
     */
    protected void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        if (ctx.isDaemon() || data.commonData() == null)
            return;

        ServicesCommonDiscoveryData initData = (ServicesCommonDiscoveryData)data.commonData();

        srvcsToStart = initData.servicesToStart();

        ctx.service().servicesInfo(initData.servicesDescriptors());

        initData.exchangeQueue().forEach(t -> ctx.service().exchange().addEvent(t.event(), t.topologyVersion(),
            t.exchangeId()));
    }

    /**
     * Gets and remove services received to deploy from node with given id on joining.
     *
     * @param nodeId Joined node id.
     * @return List of services to deploy received on node joining with given id. Possible {@code null} if nothing
     * received.
     */
    @Nullable protected List<ServiceInfo> getAndRemoveServicesReceivedFromJoin(UUID nodeId) {
        ArrayList<ServiceInfo> srvcs = new ArrayList<>();

        synchronized (changeInfoMux) {
            srvcsToStart.removeIf(info -> {
                if (info.originNodeId().equals(nodeId)) {
                    srvcs.add(info);

                    return true;
                }

                return false;
            });

            return srvcs;
        }
    }

    /**
     * Gets and remove all services received to deploy from nodes on joining.
     *
     * @return List of services to deploy received on nodes joining.
     */
    protected List<ServiceInfo> getAndRemoveServicesReceivedFromJoin() {
        assert srvcsToStart != null;

        synchronized (changeInfoMux) {
            ArrayList<ServiceInfo> srvcs = new ArrayList<>(srvcsToStart);

            srvcsToStart.clear();

            return srvcs;
        }
    }

    /**
     * Handles the first node start, that means {@link #onGridDataReceived(DiscoveryDataBag.GridDiscoveryData)} has not
     * been called.
     */
    protected void onFirstNodeStart() {
        assert locJoiningData != null;

        srvcsToStart = new ArrayList<>(locJoiningData.staticSrvcsInfo);
    }
}
