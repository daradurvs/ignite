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

package org.apache.ignite.services;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class DummyDeployTest extends GridCommonAbstractTest {
    /** Service name. */
    private static final String SERVICE_NAME = "testServiceDeploy";

    /** */
    public void testDeploy() throws Exception {
        try {
            Ignite i0 = startGrid(0);

            Ignite i1 = startGrid(1);

            IgniteServices services = i1.services();

            services.deployNodeSingleton(SERVICE_NAME, new TestService());

            GridTestUtils.waitForCondition(() -> services.service(SERVICE_NAME) != null, 20_000);

            TestService svc = services.service(SERVICE_NAME);

            assertNotNull(svc);
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    public void testDeploy2() throws Exception {
        try {
            Ignite i0 = startGrid(0);

            IgniteServices services = i0.services();

            services.deployClusterSingleton(SERVICE_NAME, new TestService());

            GridTestUtils.waitForCondition(() -> services.service(SERVICE_NAME) != null, 20_000);

            TestService svc = services.service(SERVICE_NAME);

            assertNotNull(svc);
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    public void testUndeploy() throws Exception {
        try {
            Ignite i0 = startGrid(0);

            IgniteServices services = i0.services();

            services.deployClusterSingleton(SERVICE_NAME, new TestService());

            GridTestUtils.waitForCondition(() -> services.service(SERVICE_NAME) != null, 20_000);

            TestService svc = services.service(SERVICE_NAME);

            assertNotNull(svc);

            services.cancel(SERVICE_NAME);

            svc = services.service(SERVICE_NAME);

            assertNull(svc);
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    private static class TestService implements Service {
        /** */
        @IgniteInstanceResource
        Ignite ignite;

        /** */
        public void printNodeId() {
            ignite.log().info("*** TEST " + ignite.cluster().localNode().id());
        }

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {

        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {

        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {

        }
    }
}
