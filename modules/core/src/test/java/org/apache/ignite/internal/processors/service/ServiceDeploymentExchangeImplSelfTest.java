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
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests of {@link ServicesDeploymentExchangeManagerImpl}.
 */
public class ServiceDeploymentExchangeImplSelfTest {
    /** */
    @BeforeClass
    public static void setup() {
        GridTestProperties.init();
    }

    /**
     *
     */
    @Test
    public void testInsertFirstTasksInEmptyQueue() {
        ServicesDeploymentExchangeManagerImpl exchMgr = manager();

        ArrayDeque<ServicesDeploymentExchangeTask> tasks = new ArrayDeque<>();

        for (int i = 0; i < 5; i++)
            tasks.add(new TestTaskClass());

        assertEquals(0, exchMgr.tasks().size());

        exchMgr.insertFirst(tasks);

        assertEquals(tasks.size(), exchMgr.tasks().size());

        for (ServicesDeploymentExchangeTask task : exchMgr.tasks())
            Assert.assertSame(tasks.poll(), task);
    }

    /**
     *
     */
    @Test
    public void testInsertFirstTasksInNotEmptyQueue() {
        ServicesDeploymentExchangeManagerImpl exchMgr = manager();

        ServicesDeploymentExchangeTask t1 = new TestTaskClass();

        exchMgr.tasks().add(t1);

        ServicesDeploymentExchangeTask t2 = new TestTaskClass();

        exchMgr.tasks().add(t2);

        assertEquals(2, exchMgr.tasks().size());

        ArrayDeque<ServicesDeploymentExchangeTask> tasks = new ArrayDeque<>();

        for (int i = 0; i < 5; i++)
            tasks.add(new TestTaskClass());

        exchMgr.insertFirst(tasks);

        assertEquals(tasks.size() + 2, exchMgr.tasks().size());

        assertSame(t2, exchMgr.tasks().pollLast());

        assertSame(t1, exchMgr.tasks().pollLast());

        assertEquals(tasks.size(), exchMgr.tasks().size());

        for (ServicesDeploymentExchangeTask task : exchMgr.tasks())
            Assert.assertSame(tasks.poll(), task);
    }

    /**
     * @return Instance of ServicesDeploymentExchangeManagerImpl.
     */
    private ServicesDeploymentExchangeManagerImpl manager() {
        return new ServicesDeploymentExchangeManagerImpl(mockKernalContext());
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
     * Service deployment exchange task no-op implementation for tests.
     */
    private static class TestTaskClass implements ServicesDeploymentExchangeTask {
        /** {@inheritDoc} */
        @Override public void init(GridKernalContext kCtx) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void event(DiscoveryEvent evt, AffinityTopologyVersion evtTopVer) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public DiscoveryEvent event() {
            // No-op.
            return null;
        }

        /** {@inheritDoc} */
        @Override public ServicesDeploymentExchangeId exchangeId() {
            return new ServicesDeploymentExchangeId();
        }

        /** {@inheritDoc} */
        @Override public AffinityTopologyVersion topologyVersion() {
            // No-op.
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<UUID> remaining() {
            // No-op.
            return null;
        }

        /** {@inheritDoc} */
        @Override public void complete(@Nullable Throwable err, boolean cancel) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean isCompleted() {
            // No-op.
            return false;
        }

        /** {@inheritDoc} */
        @Override public void waitForComplete(long timeout) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onReceiveSingleMapMessage(UUID snd, ServicesSingleMapMessage msg) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onReceiveFullMapMessage(UUID snd, ServicesFullMapMessage msg) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onNodeLeft(UUID nodeId) {
            // No-op.
        }
    }
}
