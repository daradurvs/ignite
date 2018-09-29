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
import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.services.ServiceConfiguration;

/**
 * Services joining node discovery data container.
 */
public class ServicesJoiningNodeDiscoveryData implements Serializable {
    /** Static services configurations container. */
    public ArrayList<ServiceConfigurationContainer> staticCfgs;

    /**
     * @param cfgs Static services configurations.
     */
    public ServicesJoiningNodeDiscoveryData(Collection<ServiceConfiguration> cfgs) {
        this.staticCfgs = new ArrayList<>();

        for (ServiceConfiguration cfg : cfgs)
            staticCfgs.add(new ServiceConfigurationContainer(IgniteUuid.randomUuid(), cfg));
    }

    /**
     * Static service configuration container.
     */
    public static class ServiceConfigurationContainer implements Serializable {
        /** Service id. */
        private final IgniteUuid srvcId;

        /** Service configuration. */
        private final ServiceConfiguration cfg;

        /**
         * @param srvcId Service id.
         * @param cfg Service configuration.
         */
        public ServiceConfigurationContainer(IgniteUuid srvcId, ServiceConfiguration cfg) {
            this.cfg = cfg;
            this.srvcId = srvcId;
        }

        /**
         * @return Service id.
         */
        public IgniteUuid serviceId() {
            return srvcId;
        }

        /**
         * @return Service configuration.
         */
        public ServiceConfiguration configuration() {
            return cfg;
        }
    }
}