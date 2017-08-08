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

package org.apache.ignite.compatibility.testframework.junits;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.compatibility.testframework.plugins.TestCompatibilityPluginProvider;
import org.apache.ignite.compatibility.testframework.util.MavenUtils;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.lang.IgnitePredicateX;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;

/**
 * Super class for all compatibility tests.
 */
public abstract class IgniteCompatibilityAbstractTest extends GridCommonAbstractTest {
    /** Local JVM Ignite node. */
    protected Ignite locJvmInstance = null;

    /** Remote JVM Ignite instance. */
    protected Ignite rmJvmInstance = null;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        TestCompatibilityPluginProvider.enable();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        TestCompatibilityPluginProvider.disable();
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /**
     * Starts new grid of given version and index <b>in separate JVM</b>.
     *
     * Uses an ignite-core artifact in the Maven local repository, if it isn't exists there, it will be downloaded and
     * stored via Maven.
     *
     * @param idx Instance index.
     * @param ver Ignite version.
     * @param clos IgniteInClosure for post-configuration.
     * @return Started grid.
     * @throws Exception If failed.
     */
    protected IgniteEx startGrid(int idx, String ver, IgniteInClosure<IgniteConfiguration> clos) throws Exception {
        return startGrid(getTestIgniteInstanceName(idx), ver, clos);
    }

    /**
     * Starts new grid of given version and name <b>in separate JVM</b>.
     *
     * Uses an ignite-core artifact in the Maven local repository, if it isn't exists there, it will be downloaded and
     * stored via Maven.
     *
     * @param igniteInstanceName Instance name.
     * @param ver Ignite version.
     * @param clos IgniteInClosure for post-configuration.
     * @return Started grid.
     * @throws Exception If failed.
     */
    protected IgniteEx startGrid(final String igniteInstanceName, final String ver,
        IgniteInClosure<IgniteConfiguration> clos) throws Exception {
        assert isMultiJvm() : "MultiJvm mode must be switched on for the node stop properly.";

        assert !igniteInstanceName.equals(getTestIgniteInstanceName(0)) : "Use default instance name for local nodes only!";

        final String closPath = CompatibilityTestIgniteNodeRunner.storeToFile(clos);

        final IgniteConfiguration cfg = getConfiguration(igniteInstanceName); // won't use to start node

        IgniteEx ignite = new IgniteProcessProxy(cfg, log, locJvmInstance, true) {
            @Override protected String igniteNodeRunnerClassName() throws Exception {
                return CompatibilityTestIgniteNodeRunner.class.getCanonicalName();
            }

            @Override protected String params(IgniteConfiguration cfg, boolean resetDiscovery) throws Exception {
                return closPath + " " + igniteInstanceName + " " + getId();
            }

            @Override protected Collection<String> filteredJvmArgs() throws Exception {
                Collection<String> filteredJvmArgs = new ArrayList<>();

                filteredJvmArgs.add("-ea");

                for (String arg : U.jvmArgs()) {
                    if (arg.startsWith("-Xmx") || arg.startsWith("-Xms"))
                        filteredJvmArgs.add(arg);
                }

                String classPath = System.getProperty("java.class.path");

                String[] paths = classPath.split(File.pathSeparator);

                StringBuilder pathBuilder = new StringBuilder();

                String corePathTemplate = "ignite.modules.core.target.classes".replace(".", File.separator);
                String coreTestsPathTemplate = "ignite.modules.core.target.test-classes".replace(".", File.separator);

                for (String path : paths) {
                    if (!path.contains(corePathTemplate) && !path.contains(coreTestsPathTemplate))
                        pathBuilder.append(path).append(File.pathSeparator);
                }

                String pathToArtifact = MavenUtils.getPathToIgniteCoreArtifact(ver);
                pathBuilder.append(pathToArtifact).append(File.pathSeparator);

                String pathToTestsArtifact = MavenUtils.getPathToIgniteCoreArtifact(ver, "tests");
                pathBuilder.append(pathToTestsArtifact).append(File.pathSeparator);

                filteredJvmArgs.add("-cp");
                filteredJvmArgs.add(pathBuilder.toString());

                return filteredJvmArgs;
            }
        };

        if (rmJvmInstance == null)
            rmJvmInstance = ignite;

        return ignite;
    }

    /** {@inheritDoc} */
    @Override protected Ignite startGrid(String igniteInstanceName, IgniteConfiguration cfg,
        GridSpringResourceContext ctx)
        throws Exception {
        Ignite ignite = super.startGrid(igniteInstanceName, cfg, ctx);

        // if started node isn't first node in the local JVM then it was checked earlier for join to topology
        if (rmJvmInstance != null && locJvmInstance == null) {
            CountDownLatch nodeJoinedLatch = new CountDownLatch(1);

            ignite.events().localListen(new ChangedTopologyVersionListener(nodeJoinedLatch), EventType.EVT_NODE_METRICS_UPDATED);

            assert nodeJoinedLatch.await(30, TimeUnit.SECONDS) : "Node has not joined [id=" + ignite.configuration().getNodeId() + "]";
        }

        if (locJvmInstance == null && !isRemoteJvm(igniteInstanceName))
            locJvmInstance = ignite;

        return ignite;
    }

    /** */
    protected static class ChangedTopologyVersionListener extends IgnitePredicateX<Event> {
        /** Node joined latch. */
        private final CountDownLatch nodeJoinedLatch;

        /**
         * @param nodeJoinedLatch Node joined latch.
         */
        ChangedTopologyVersionListener(CountDownLatch nodeJoinedLatch) {
            this.nodeJoinedLatch = nodeJoinedLatch;
        }

        /** {@inheritDoc} */
        @Override public boolean applyx(Event e) {
            if (((DiscoveryEvent)e).topologyVersion() != 1) {
                nodeJoinedLatch.countDown();

                return false;
            }

            return true;
        }
    }
}
