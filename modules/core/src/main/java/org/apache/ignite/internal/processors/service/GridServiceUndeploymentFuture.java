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
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Service deployment future.
 */
public class GridServiceUndeploymentFuture extends GridFutureAdapter<Object> {
    /** */
    private final String name;

    /**
     * @param name Service name.
     */
    public GridServiceUndeploymentFuture(String name) {
        this.name = name;
    }

    /**
     * @return Service name.
     */
    public String getName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridServiceUndeploymentFuture fut = (GridServiceUndeploymentFuture)o;

        return Objects.equals(name, fut.name);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(name);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridServiceUndeploymentFuture.class, this);
    }
}
