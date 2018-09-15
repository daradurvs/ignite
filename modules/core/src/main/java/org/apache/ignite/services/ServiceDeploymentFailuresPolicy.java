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

package org.apache.ignite.services;

import org.jetbrains.annotations.Nullable;

/**
 * Policy that defines how the coordinator will react on a failure of a service's deployment or reassignment.
 */
public enum ServiceDeploymentFailuresPolicy {
    /**
     * When service deployment failures policy is {@code CANCEL} and there are errors on deployment/reassignment
     * requests, coordinator will initiate cancellation of existing services in Ingite cluster and propogates deployment
     * errors to all nodes.
     */
    CANCEL,

    /**
     * When service deployment failures policy is {@code IGNORE} and there are errors on deployment/reassignment
     * requests, coordinator will ignore them and services will be deployed as is and propogates deployment errors to
     * all nodes.
     */
    IGNORE;

    /** Enumerated values. */
    private static final ServiceDeploymentFailuresPolicy[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static ServiceDeploymentFailuresPolicy fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
