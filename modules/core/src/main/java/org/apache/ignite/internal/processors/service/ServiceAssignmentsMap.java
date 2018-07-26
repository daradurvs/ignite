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

/**
 * Single service assignments unit to send over network.
 */
public class ServiceAssignmentsMap implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Service name. */
    private String name;

    /** Service assignments. */
    @GridToStringInclude
    private Map<UUID, Integer> assigns;

    /** Topology version. */
    private long topVer;

    /**
     * @param name Service name.
     * @param assigns Service assignments.
     * @param topVer Topology version.
     */
    public ServiceAssignmentsMap(String name, Map<UUID, Integer> assigns, long topVer) {
        this.name = name;
        this.assigns = assigns;
        this.topVer = topVer;
    }

    /**
     * @return Service assignments.
     */
    public Map<UUID, Integer> assigns() {
        return assigns;
    }

    /**
     * @param assigns Service assignments.
     */
    public void assigns(Map<UUID, Integer> assigns) {
        this.assigns = assigns;
    }

    /**
     * @return Service name.
     */
    public String name() {
        return name;
    }

    /**
     * @return Topology version.
     */
    public long topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServiceAssignmentsMap.class, this);
    }
}
