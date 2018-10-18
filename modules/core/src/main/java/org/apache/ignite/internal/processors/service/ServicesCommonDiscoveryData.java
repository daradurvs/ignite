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

import java.io.Serializable;
import java.util.ArrayList;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * Initial data container to send on joined node.
 */
class ServicesCommonDiscoveryData implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Clusters registered services descriptors. */
    private final ArrayList<ServiceInfo> registeredSrvcs;

    /**
     * @param registeredSrvcs Clusters registered services descriptors.
     */
    public ServicesCommonDiscoveryData(@NotNull ArrayList<ServiceInfo> registeredSrvcs) {
        this.registeredSrvcs = registeredSrvcs;
    }

    /**
     * Returns clusters registered services descriptors.
     *
     * @return Deployed services descriptors.
     */
    public ArrayList<ServiceInfo> registeredServices() {
        return registeredSrvcs;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServicesCommonDiscoveryData.class, this);
    }
}
