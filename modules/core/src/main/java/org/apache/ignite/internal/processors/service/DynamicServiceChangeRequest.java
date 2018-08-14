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
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.services.ServiceConfiguration;

/**
 * Service deployment/undeployment request.
 */
public class DynamicServiceChangeRequest implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Request id. */
    private IgniteUuid reqId = IgniteUuid.randomUuid();

    /** Change's initiator id. */
    private UUID nodeId;

    /** Service id. */
    private IgniteUuid srvcId;

    /** Service configuration. */
    private ServiceConfiguration cfg;

    /** If this is deployment request. */
    private boolean deploy;

    /** If this is undeployment request. */
    private boolean undeploy;

    /**
     * @param nodeId Change's initiator id.
     * @param srvcId Service id.
     */
    public DynamicServiceChangeRequest(UUID nodeId, IgniteUuid srvcId) {
        this.nodeId = nodeId;
        this.srvcId = srvcId;
    }

    /**
     * @param nodeId Change's initiator id.
     * @param srvcId Service id.
     * @param cfg Service configuration.
     * @return Deployment request.
     */
    public static DynamicServiceChangeRequest deploymentRequest(UUID nodeId, IgniteUuid srvcId,
        ServiceConfiguration cfg) {
        DynamicServiceChangeRequest req = new DynamicServiceChangeRequest(nodeId, srvcId);

        req.configuration(cfg);
        req.deploy(true);

        return req;
    }

    /**
     * @param nodeId Change's initiator id.
     * @param srvcId Service id.
     * @return Deployment request.
     */
    public static DynamicServiceChangeRequest undeploymentRequest(UUID nodeId, IgniteUuid srvcId) {
        DynamicServiceChangeRequest req = new DynamicServiceChangeRequest(nodeId, srvcId);

        req.undeploy(true);

        return req;
    }

    /**
     * @return Request id.
     */
    public IgniteUuid requestId() {
        return reqId;
    }

    /**
     * @param reqId New request id.
     */
    public void requestId(IgniteUuid reqId) {
        this.reqId = reqId;
    }

    /**
     * @return Change's initiator id.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId New change's initiator id.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return Service id.
     */
    public IgniteUuid serviceId() {
        return srvcId;
    }

    /**
     * @param srvcId New service id.
     */
    public void serviceId(IgniteUuid srvcId) {
        this.srvcId = srvcId;
    }

    /**
     * @return Service configuration.
     */
    public ServiceConfiguration configuration() {
        return cfg;
    }

    /**
     * @param cfg New service configuration.
     */
    public void configuration(ServiceConfiguration cfg) {
        this.cfg = cfg;
    }

    /**
     * @return If this is deployment request.
     */
    public boolean deploy() {
        return deploy;
    }

    /**
     * @param deploy New deployment request flag.
     */
    public void deploy(boolean deploy) {
        this.deploy = deploy;
    }

    /**
     * @return If this is undeployment request.
     */
    public boolean undeploy() {
        return undeploy;
    }

    /**
     * @param undeploy New undeployment request flag.
     */
    public void undeploy(boolean undeploy) {
        this.undeploy = undeploy;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DynamicServiceChangeRequest.class, this);
    }
}
