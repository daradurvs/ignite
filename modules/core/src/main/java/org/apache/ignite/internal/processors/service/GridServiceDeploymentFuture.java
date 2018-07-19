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

import java.util.Objects;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.services.ServiceConfiguration;

/**
 * Service deployment future.
 */
public class GridServiceDeploymentFuture extends GridFutureAdapter<Object> {
    /** Service configuration. */
    private final ServiceConfiguration cfg;

    /**
     * @param cfg Service configuration.
     */
    public GridServiceDeploymentFuture(ServiceConfiguration cfg) {
        this.cfg = cfg;
    }

    /**
     * @return Service configuration.
     */
    public ServiceConfiguration configuration() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridServiceDeploymentFuture fut = (GridServiceDeploymentFuture)o;

        return cfg.equalsIgnoreNodeFilter(fut.cfg);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(cfg);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridServiceDeploymentFuture.class, this);
    }
}
