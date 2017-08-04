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

package org.apache.ignite.testframework.junits.campatibility;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.tests.compatibility.MavenUtils;

/**
 * Run Ignite node.
 */
public class IgniteNodeRunner2 {
    /** */
    private static volatile Ignite ignite;

    /**
     * Starts {@link Ignite} with test's default configuration.
     *
     * @param args Arguments.
     * @throws Exception In case of an error.
     */
    public static void main(String[] args) throws Exception {
        IgniteConfiguration cfg = CompatibilityTestsFacade.getConfiguration();

        System.out.println(MavenUtils.getPathToIgniteCoreArtifact("2.1.0", "tests"));

//        ignite = Ignition.start(cfg);
    }

    /**
     * @return Ignite instance started at main.
     */
    public static IgniteEx startedInstance() {
        return (IgniteEx)ignite;
    }

    /**
     * @return {@code True} if there is ignite node started via {@link IgniteNodeRunner2} at this JVM.
     */
    public static boolean hasStartedInstance() {
        return ignite != null;
    }
}
