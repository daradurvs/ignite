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
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.jetbrains.annotations.Nullable;

/**
 * Defines methods to manage a task of service deployment exchange.
 */
public interface ServicesDeploymentExchangeTask extends Serializable {
    /**
     * Initializes exchange task.
     *
     * @param kCtx Kernal context.
     * @throws IgniteCheckedException In case of an error.
     */
    public void init(GridKernalContext kCtx) throws IgniteCheckedException;

    /**
     * Returns cause discovery event type id.
     *
     * @return Event's type id.
     */
    public int eventTypeId();

    /**
     * Returns cause discovery event node id.
     *
     * @return Node id.
     */
    public UUID nodeId();

    /**
     * Sets discovery custom message.
     *
     * @param customMsg Discovery custom message.
     */
    public void customMessage(DiscoveryCustomMessage customMsg);

    /**
     * Returns discovery custom event's message.
     *
     * @return Discovery custom event's message.
     */
    public DiscoveryCustomMessage customMessage();

    /**
     * Returns services deployment exchange id of the task.
     *
     * @return Services deployment exchange id.
     */
    public ServicesDeploymentExchangeId exchangeId();

    /**
     * Returns cause of exchange topology version.
     *
     * @return Cause of exchange topology version.
     */
    public AffinityTopologyVersion topologyVersion();

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
     */
    public void complete(@Nullable Throwable err);

    /**
     * Returns if the task completed.
     *
     * @return {@code true} if the task completed, otherwise {@code false}.
     */
    public boolean isCompleted();

    /**
     * Synchronously waits for completion of the task for up to the given timeout.
     *
     * @param timeout The maximum time to wait in milliseconds.
     * @throws IgniteCheckedException In case of an error.
     */
    public void waitForComplete(long timeout) throws IgniteCheckedException;

    /**
     * Releases resources to reduce memory consumption.
     */
    public void clear();

    /**
     * Handles received single node services map message.
     *
     * @param snd Sender node id.
     * @param msg Single services map message.
     */
    public void onReceiveSingleMapMessage(UUID snd, ServicesSingleMapMessage msg);

    /**
     * Handles received full services map message.
     *
     * @param snd Sender node id.
     * @param msg Full services map message.
     */
    public void onReceiveFullMapMessage(UUID snd, ServicesFullMapMessage msg);

    /**
     * Handles situations when node leaves topology.
     *
     * @param nodeId Left node id.
     */
    public void onNodeLeft(UUID nodeId);
}
