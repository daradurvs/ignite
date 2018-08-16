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
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Single service deployments.
 */
public class ServiceDeploymentsMap implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Service name. */
    private IgniteUuid srvcId;

    /** Service topology snapshot. */
    @GridToStringInclude
    private Map<UUID, Integer> top;

    /** Topology version. */
    private long topVer;

    /**
     * @param srvcId Service id.
     * @param top Service topology snapshot.
     * @param topVer Topology version.
     */
    public ServiceDeploymentsMap(IgniteUuid srvcId, Map<UUID, Integer> top, long topVer) {
        this.srvcId = srvcId;
        this.top = top;
        this.topVer = topVer;
    }

    /**
     * @return Service id.
     */
    public IgniteUuid serviceId() {
        return srvcId;
    }

    /**
     * @return Topology version.
     */
    public long topologyVersion() {
        return topVer;
    }

    /**
     * @return Service topology snapshot.
     */
    public Map<UUID, Integer> topologySnapshot() {
        return top;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServiceDeploymentsMap.class, this);
    }
}
