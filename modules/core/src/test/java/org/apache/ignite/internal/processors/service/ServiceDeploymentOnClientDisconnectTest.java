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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cluster.ClusterGroupAdapter;
import org.apache.ignite.internal.processors.service.inner.LongInitializedTestService;
import org.apache.ignite.internal.processors.service.inner.MyServiceFactory;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;

/**
 * Test service deployment while client node disconnecting.
 */
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class ServiceDeploymentOnClientDisconnectTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (cfg.getDiscoverySpi() instanceof TcpDiscoverySpi)
            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setClientReconnectDisabled(false);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid(0);

        startGrid(getConfiguration("client").setClientMode(true));

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception In case of an error.
     */
    public void testInitiatorDeploymentFutureCompletionOnClientDisconnectTest() throws Exception {
        IgniteFuture fut = client().services().deployNodeSingletonAsync("testService",
            new LongInitializedTestService(5_000L));

        server().close();

        GridTestUtils.assertThrowsWithCause((Runnable)fut::get, IgniteClientDisconnectedException.class);
    }

    /**
     * @throws Exception In case of an error.
     */
    public void testThrowingExceptionOnDeployUsingPuplicApiWhileClientDisconnectedTest() throws Exception {
        runTest(() -> {
            GridTestUtils.assertThrowsWithCause(
                () -> client().services().deployNodeSingletonAsync("testService", MyServiceFactory.create()),
                IgniteClientDisconnectedException.class);
        });
    }

    /**
     * @throws Exception In case of an error.
     */
    public void testThrowingExceptionOnUndeployUsingPuplicApiWhileClientDisconnectedTest() throws Exception {
        runTest(() -> {
            GridTestUtils.assertThrowsWithCause(() -> client().services().cancelAll(),
                IgniteClientDisconnectedException.class);
        });
    }

    /**
     * @throws Exception In case of an error.
     */
    public void testThrowingExceptionOnDeployUsingInternalApiWhileClientDisconnectedTest() throws Exception {
        runTest(() -> {
            GridTestUtils.assertThrowsWithCause(() -> client().context().service().deployNodeSingleton(
                new ClusterGroupAdapter(), "testService", MyServiceFactory.create()).get(),
                IgniteClientDisconnectedCheckedException.class);
        });
    }

    /**
     * @throws Exception In case of an error.
     */
    public void testThrowingExceptionOnUndeployUsingInternalApiWhileClientDisconnectedTest() throws Exception {
        runTest(() -> {
            GridTestUtils.assertThrowsWithCause(() -> client().context().service().cancelAll().get(),
                IgniteClientDisconnectedCheckedException.class);
        });
    }

    /**
     * Apply given task on disconnected client node.
     *
     * @param task Task.
     * @throws InterruptedException If interrupted.
     */
    private void runTest(final Runnable task) throws InterruptedException {
        Ignite client = client();

        CountDownLatch latch = new CountDownLatch(1);

        client.events().localListen((IgnitePredicate<Event>)evt -> {
            latch.countDown();

            return false;
        }, EVT_CLIENT_NODE_DISCONNECTED);

        server().close();

        assertTrue(latch.await(10_000L, TimeUnit.MILLISECONDS));

        task.run();
    }

    /**
     * @return Clients node.
     */
    private IgniteEx client() {
        return grid("client");
    }

    /**
     * @return Server node.
     */
    private IgniteEx server() {
        return grid(0);
    }
}
