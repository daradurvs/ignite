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
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.ignite.internal.processors.service.ServicesDeploymentExchangeManager.exchangeId;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests of {@link ServicesDeploymentExchangeManager}.
 */
public class ServiceDeploymentExchangeManagerSelfTest {
    /** */
    @BeforeClass
    public static void setup() {
        GridTestProperties.init();
    }

    /** */
    @Test
    public void testAddTaskInEmptyQueue() {
        ServicesDeploymentExchangeManager exchMgr = manager();

        ArrayDeque<ServicesDeploymentExchangeTask> tasks = new ArrayDeque<>();

        for (int i = 0; i < 5; i++)
            tasks.add(randomExchangeTask());

        assertEquals(0, exchMgr.tasks().size());

        tasks.forEach(t -> exchMgr.addTask(t.event(), t.topologyVersion()));

        assertEquals(tasks.size(), exchMgr.tasks().size());
    }

    /** */
    @Test
    public void testAddTasksInNotEmptyQueue() {
        ServicesDeploymentExchangeManager exchMgr = manager();

        ServicesDeploymentExchangeTask t1 = randomExchangeTask();

        exchMgr.tasks().add(t1);

        ServicesDeploymentExchangeTask t2 = randomExchangeTask();

        exchMgr.tasks().add(t2);

        assertEquals(2, exchMgr.tasks().size());

        ArrayDeque<ServicesDeploymentExchangeTask> tasks = new ArrayDeque<>();

        for (int i = 0; i < 5; i++)
            tasks.add(randomExchangeTask());

        tasks.forEach(t -> exchMgr.addTask(t.event(), t.topologyVersion()));

        assertEquals(tasks.size() + 2, exchMgr.tasks().size());
    }

    /**
     * @return Instance of ServicesDeploymentExchangeManager.
     */
    private ServicesDeploymentExchangeManager manager() {
        return new ServicesDeploymentExchangeManager(mockKernalContext());
    }

    /**
     * @return Mocked GridKernalContext.
     */
    private GridKernalContext mockKernalContext() {
        GridTestKernalContext spyCtx = spy(new GridTestKernalContext(new GridTestLog4jLogger()));

        GridEventStorageManager mockEvt = mock(GridEventStorageManager.class);
        GridIoManager mockIo = mock(GridIoManager.class);

        when(spyCtx.event()).thenReturn(mockEvt);
        when(spyCtx.io()).thenReturn(mockIo);

        return spyCtx;
    }

    /**
     * @return Service deployment exchange id.
     */
    private ServicesDeploymentExchangeTask randomExchangeTask() {
        DiscoveryEvent evt = new DiscoveryEvent(
            new GridTestNode(UUID.randomUUID()), "", 10, new GridTestNode(UUID.randomUUID()));

        AffinityTopologyVersion topVer = new AffinityTopologyVersion(ThreadLocalRandom.current().nextLong());

        ServicesDeploymentExchangeTask task = new ServicesDeploymentExchangeTask(exchangeId(evt, topVer));

        task.onEvent(evt, topVer);

        return task;
    }
}
