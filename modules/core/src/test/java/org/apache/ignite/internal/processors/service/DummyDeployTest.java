package org.apache.ignite.internal.processors.service;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class DummyDeployTest extends GridCommonAbstractTest {
    private static final String SERVICE_NAME = "testServiceDeploy";

    public DummyDeployTest() {
    }

    public void testDeploy() throws Exception {
        try {
            Ignite i0 = startGrid(0);

            Ignite i1 = startGrid(1);

            IgniteServices services = i1.services();

            services.deployNodeSingleton("testServiceDeploy", new DummyDeployTest.TestService());

            DummyDeployTest.TestService svc = services.service("testServiceDeploy");

            assertNotNull(svc);
        }
        finally {
            stopAllGrids();
        }
    }

    public void testDeploy2() throws Exception {
        try {
            Ignite i0 = startGrid(0);
            IgniteServices services = i0.services();
            services.deployClusterSingleton("testServiceDeploy", new DummyDeployTest.TestService());

            DummyDeployTest.TestService svc = services.service("testServiceDeploy");

            assertNotNull(svc);
        }
        finally {
            stopAllGrids();
        }

    }

    public void testUndeploy() throws Exception {
        try {
            Ignite i0 = startGrid(0);

            IgniteServices services = i0.services();

            services.deployClusterSingleton("testServiceDeploy", new DummyDeployTest.TestService());

            DummyDeployTest.TestService svc = services.service("testServiceDeploy");

            assertNotNull(svc);

            services.cancel("testServiceDeploy");

            svc = services.service("testServiceDeploy");

            assertNull(svc);
        }
        finally {
            stopAllGrids();
        }

    }

    public void testClientsDeploy() throws Exception {
        try {
            Ignite i0 = startGrid(0);

            Ignite client = startGrid(getConfiguration("client").setClientMode(true));

            IgniteServices services = client.services();

            services.deployNodeSingleton("testServiceDeploy", new TestService());

            Service svc = services.serviceProxy("testServiceDeploy", Service.class, false);

            assertNotNull(svc);
        }
        finally {
            stopAllGrids();
        }
    }

    private static class TestService implements Service {
        @IgniteInstanceResource
        Ignite ignite;

        private TestService() {
        }

        public void printNodeId() {
            ignite.log().info("*** TEST " + ignite.cluster().localNode().id());
        }

        public void cancel(ServiceContext ctx) {
        }

        public void init(ServiceContext ctx) throws Exception {
        }

        public void execute(ServiceContext ctx) throws Exception {
        }
    }
}
