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

import com.thoughtworks.xstream.XStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.compatibility.testframework.plugins.TestCompatibilityPluginProvider;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.multijvm.IgniteNodeRunner;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Runs Ignite node.
 */
public class CompatibilityTestIgniteNodeRunner extends IgniteNodeRunner {
    /** */
    private static final String IGNITE_CLOSURE_FILE = System.getProperty("java.io.tmpdir") +
        File.separator + "igniteClosure.tmp_";

    /**
     * Starts {@link Ignite} with test's default configuration.
     *
     * @param args Arguments.
     * @throws Exception In case of an error.
     */
    public static void main(String[] args) throws Exception {
        X.println(GridJavaProcess.PID_MSG_PREFIX + U.jvmPid());

        X.println("Starting Ignite Node... Args=" + Arrays.toString(args));

        if (args.length < 3)
            throw new IllegalArgumentException("At least four arguments expected:" +
                " [path/to/closure/file] [ignite-instance-name] [node-uuid] [sync-node-uuid] [optional/path/to/closure/file]");

        TestCompatibilityPluginProvider.enable();

        IgniteConfiguration cfg = CompatibilityTestsFacade.getConfiguration();

        IgniteInClosure<IgniteConfiguration> cfgClos = readClosureFromFileAndDelete(args[0]);

        cfgClos.apply(cfg);

        final UUID id = UUID.fromString(args[2]);
        final UUID syncId = UUID.fromString(args[3]);

        // Ignite instance name and id must be set according to arguments
        // it's used for nodes managing: start, stop etc.
        cfg.setIgniteInstanceName(args[1]);
        cfg.setNodeId(id);

        final Ignite ignite = Ignition.start(cfg);

        // If 'id' equals 'syncId' then the starting node is the first node and
        // there was no need to check the join to topology.
        if (!id.equals(syncId)) {
            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    boolean found = ignite.cluster().node(syncId) != null;

                    if (found)
                        X.println(IgniteCompatibilityAbstractTest.SYNCHRONIZATION_MESSAGE_JOINED + id);

                    return found;
                }
            }, 20);
        }

        // It needs to set private static field 'ignite' of the IgniteNodeRunner class via reflection
        GridTestUtils.setFieldValue(new IgniteNodeRunner(), "ignite", ignite);

        if (args.length == 5) {
            IgniteInClosure<Ignite> iClos = readClosureFromFileAndDelete(args[4]);

            iClos.apply(ignite);
        }

        X.println(IgniteCompatibilityAbstractTest.SYNCHRONIZATION_MESSAGE_PREPARED + id);
    }

    /**
     * Stores {@link IgniteInClosure} to file as xml.
     *
     * @param clos IgniteInClosure.
     * @return A name of file where the closure was stored.
     * @throws IOException In case of an error.
     * @see #readClosureFromFileAndDelete(String)
     */
    @Nullable public static String storeToFile(@Nullable IgniteInClosure clos) throws IOException {
        if (clos == null)
            return null;

        String fileName = IGNITE_CLOSURE_FILE + clos.hashCode();

        storeToFile(clos, fileName);

        return fileName;
    }

    /**
     * Stores {@link IgniteInClosure} to file as xml.
     *
     * @param clos IgniteInClosure.
     * @param fileName A name of file where the closure was stored.
     * @throws IOException In case of an error.
     * @see #readClosureFromFileAndDelete(String)
     */
    public static void storeToFile(@NotNull IgniteInClosure clos, @NotNull String fileName) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(fileName), StandardCharsets.UTF_8)) {
            new XStream().toXML(clos, writer);
        }
    }

    /**
     * Reads closure from given file name and delete the file after.
     *
     * @param fileName Closure file name.
     * @param <T> Type of closure argument.
     * @return IgniteInClosure for post-configuration.
     * @throws IOException In case of an error.
     * @see #storeToFile(IgniteInClosure, String)
     */
    @SuppressWarnings("unchecked")
    public static <T> IgniteInClosure<T> readClosureFromFileAndDelete(
        String fileName) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(fileName), StandardCharsets.UTF_8)) {
            return (IgniteInClosure<T>)new XStream().fromXML(reader);
        }
        finally {
            new File(fileName).delete();
        }
    }
}
