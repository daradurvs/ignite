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

import java.util.ArrayDeque;
import java.util.Deque;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;

/**
 * Defines methods to manage a services deployment and reassignments exchange across Ignite cluster.
 */
public interface ServicesDeploymentExchangeManager {
    /**
     * Starts processing of services deployments exchange tasks.
     */
    public void startProcessing();

    /**
     * Stops processing of services deployments exchange tasks.
     */
    public void stopProcessing();

    /**
     * Returns queue of deployments tasks.
     *
     * @return Queue of deployment tasks.
     */
    public ArrayDeque<ServicesDeploymentExchangeTask> tasks();

    /**
     * Inserts given deployments tasks in begin of queue.
     *
     * @param tasks Queue of deployments tasks.
     */
    public void insertFirst(Deque<ServicesDeploymentExchangeTask> tasks);

    /**
     * @return Ready topology version.
     */
    public AffinityTopologyVersion readyTopologyVersion();

    /**
     * Special handler for local discovery events for which the regular events are not generated, e.g. local join and
     * client reconnect events.
     *
     * @param evt Discovery event.
     * @param discoCache Discovery cache.
     */
    public void onLocalEvent(DiscoveryEvent evt, DiscoCache discoCache);
}
