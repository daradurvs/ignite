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
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.jetbrains.annotations.Nullable;

/**
 * Defines methods to manage a task of service deployment exchange.
 */
public interface ServicesDeploymentExchangeTask extends ServicesDeploymentExchangeManageable, Serializable {
    /**
     * Initializes exchange task.
     *
     * @param ctx Kernal context.
     * @throws IgniteCheckedException In case of an error.
     */
    public void init(GridKernalContext ctx) throws IgniteCheckedException;

    /**
     * Returns services deployment exchange id of the task.
     *
     * @return Services deployment exchange id.
     */
    public ServicesDeploymentExchangeId exchangeId();

    /**
     * Returns of remaining nodes ids to receive single node assignments message.
     *
     * @return Unmodifiable collection of nodes ids to wait single node assignments messages.
     */
    public Collection<UUID> remaining();

    /**
     * Completes the task.
     *
     * @param err Error to complete with.
     * @param cancel Cancel flag.
     */
    public void complete(@Nullable Throwable err, boolean cancel);

    /**
     * Returns if the task completed.
     *
     * @return {@code true} if the task completed, otherwise {@code false}.
     */
    public boolean isComplete();

    /**
     * Synchronously waits for completion of the task for up to the given timeout.
     *
     * @param timeout The maximum time to wait in milliseconds.
     * @throws IgniteCheckedException In case of an error.
     */
    public void waitForComplete(long timeout) throws IgniteCheckedException;

    /**
     * @return Cause discovery event.
     */
    public DiscoveryEvent event();

    /**
     * @return Cause of exchange topology version.
     */
    public AffinityTopologyVersion topologyVersion();
}
