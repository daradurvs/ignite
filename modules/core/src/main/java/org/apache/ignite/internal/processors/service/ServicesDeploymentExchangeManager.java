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

import java.util.concurrent.LinkedBlockingDeque;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.jetbrains.annotations.Nullable;

/**
 * Defines methods to manage a services deployment and reassignments exchange across Ignite cluster.
 */
public interface ServicesDeploymentExchangeManager extends ServicesDeploymentExchangeManageable {
    /**
     * Handles event as cause of services assignments exchange.
     *
     * @param evt Discovery event.
     * @param topVer Affinity topology version.
     */
    public void processEvent(DiscoveryEvent evt, AffinityTopologyVersion topVer);

    /**
     * Starts processing of services deployments exchange tasks.
     */
    public void startProcessing();

    /**
     * Stops processing of services deployments exchange tasks.
     */
    public void stopProcessing();

    /**
     * @return Ready topology version.
     */
    public AffinityTopologyVersion readyTopologyVersion();

    /**
     * Gets services deployment exchange task with given id.
     *
     * @param exchId Services deployment exchange id.
     * @return Services deployment task. Possible may be {@code}.
     */
    @Nullable public ServicesDeploymentExchangeTask task(ServicesDeploymentExchangeId exchId);

    /**
     * Returns queue of deployments tasks.
     *
     * @return Queue of deployment tasks.
     */
    public LinkedBlockingDeque<ServicesDeploymentExchangeTask> tasks();

    /**
     * Inserts given deployment tasks in begin of queue.
     *
     * @param tasks Queue of deployment tasks.
     */
    public void insertToBegin(LinkedBlockingDeque<ServicesDeploymentExchangeTask> tasks);
}
