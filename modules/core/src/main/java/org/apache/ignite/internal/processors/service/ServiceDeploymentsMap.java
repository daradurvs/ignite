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

    /** Service name. */
    private String name;

    /** Service topology snapshot. */
    @GridToStringInclude
    private Map<UUID, Integer> top;

    /**
     * @param srvcId Service id.
     * @param name Service name.
     * @param top Service topology snapshot.
     */
    public ServiceDeploymentsMap(IgniteUuid srvcId, String name, Map<UUID, Integer> top) {
        this.srvcId = srvcId;
        this.name = name;
        this.top = top;
    }

    /**
     * @return Service id.
     */
    public IgniteUuid serviceId() {
        return srvcId;
    }

    /**
     * @return Service name.
     */
    public String name() {
        return name;
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
