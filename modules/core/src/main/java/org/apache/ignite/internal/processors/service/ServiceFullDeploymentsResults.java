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
 * Service full cluster deployments results.
 */
public class ServiceFullDeploymentsResults implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Service id. */
    private final IgniteUuid srvcId;

    /** Per node deployments results. */
    @GridToStringInclude
    private final Map<UUID, ServiceSingleDeploymentsResults> results;

    /**
     * @param srvcId Service id.
     * @param results Deployments results.
     */
    public ServiceFullDeploymentsResults(IgniteUuid srvcId,
        Map<java.util.UUID, ServiceSingleDeploymentsResults> results) {
        this.srvcId = srvcId;
        this.results = results;
    }

    /**
     * @return Service id.
     */
    public IgniteUuid serviceId() {
        return srvcId;
    }

    /**
     * @return Per node deployments results.
     */
    public Map<UUID, ServiceSingleDeploymentsResults> results() {
        return results;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServiceFullDeploymentsResults.class, this);
    }
}
