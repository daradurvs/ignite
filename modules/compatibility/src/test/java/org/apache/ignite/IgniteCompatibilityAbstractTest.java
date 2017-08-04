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

package org.apache.ignite.testframework.junits;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.apache.ignite.tests.compatibility.MavenUtils;

/**
 * Super class for all compatibility tests.
 */
public abstract class IgniteCompatibilityAbstractTest extends GridCommonAbstractTest {

    /**
     * Starts new grid of given version and name <b>in separate JVM</b>.
     *
     * Uses an ignite-core artifact in the Maven local repository,
     * if it isn't exists there, it will be downloaded and stored via Maven.
     *
     * @param igniteInstanceName Instance name.
     * @param ver Ignite version.
     * @param clos IgniteInClosure for post-configuration.
     * @return Started grid.
     * @throws Exception If failed.
     */
    protected Ignite startGrid(String igniteInstanceName, String ver,
        IgniteInClosure<IgniteConfiguration> clos) throws Exception {
        return startGrid(igniteInstanceName, ver, null, clos, null, true);
    }

    /**
     * Starts new grid of given version and name <b>in separate JVM</b>.
     *
     * Uses an ignite-core artifact in the Maven local repository,
     * if it isn't exists there, it will be downloaded and stored via Maven.
     *
     * @param igniteInstanceName Instance name.
     * @param ver Ignite version.
     * @param cfg Ignite configuration.
     * @param clos IgniteInClosure for post-configuration.
     * @return Started grid.
     * @throws Exception If failed.
     */
    protected Ignite startGrid(String igniteInstanceName, String ver, IgniteConfiguration cfg,
        IgniteInClosure<IgniteConfiguration> clos) throws Exception {
        return startGrid(igniteInstanceName, ver, cfg, clos, null, true);
    }

    /**
     * Starts new grid of given version and name <b>in separate JVM</b>.
     *
     * Uses an ignite-core artifact in the Maven local repository,
     * if it isn't exists there, it will be downloaded and stored via Maven.
     *
     * @param igniteInstanceName Instance name.
     * @param ver Ignite version.
     * @param cfg Ignite configuration.
     * @param clos IgniteInClosure for post-configuration.
     * @param jvmArgs Additional JVM arguments.
     * @param resetDiscovery Reset DiscoverySpi at the configuration.
     * @return Started grid.
     * @throws Exception If failed.
     */
    protected Ignite startGrid(String igniteInstanceName, final String ver, IgniteConfiguration cfg,
        IgniteInClosure<IgniteConfiguration> clos, final Collection<String> jvmArgs,
        boolean resetDiscovery) throws Exception {
        assert isMultiJvm() : "MultiJvm mode must be switched on for the node stop properly.";

        assert !isFirstGrid(igniteInstanceName) : "Please, start node of current version first.";

        if (cfg == null)
            cfg = optimize(getConfiguration(igniteInstanceName));

        final String pathToArtifact = MavenUtils.getPathToIgniteCoreArtifact(ver);

        return new IgniteProcessProxy(cfg, log, grid(0), resetDiscovery, clos) {
            @Override protected Collection<String> filteredJvmArgs() {
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

                for (String path : paths) {
                    if (!path.contains(corePathTemplate))
                        pathBuilder.append(path).append(File.pathSeparator);
                }

                pathBuilder.append(pathToArtifact).append(File.pathSeparator);

                filteredJvmArgs.add("-cp");
                filteredJvmArgs.add(pathBuilder.toString());

                // additional JVM arguments
                if (jvmArgs != null) {
                    for (String arg : jvmArgs) {
                        if (!arg.startsWith("-Xmx") && !arg.startsWith("-Xms")
                            && !arg.startsWith("-cp") && !arg.startsWith("-classpath"))
                            filteredJvmArgs.add(arg);
                    }
                }

                return filteredJvmArgs;
            }
        };
    }
}
