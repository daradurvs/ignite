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

package org.apache.ignite.marshaller;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import java.io.File;
import java.util.Collection;
import java.util.Collections;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.portable.testjar.GridPortableTestClass1;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.stream.StreamTransformer;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/** */
public class ClassloaderDelegatingTest2 extends GridCommonAbstractTest {

    protected static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    protected static final String CACHE_NAME = "cache_name";

    public static final String JAVA_CLASS_PATH = "java.class.path";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        BinaryMarshaller marsh = new BinaryMarshaller();
        cfg.setMarshaller(marsh);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(1);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setRebalanceMode(SYNC);
        cfg.setCacheConfiguration(cacheCfg);

        cfg.setPeerClassLoadingEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /**
     * @throws Exception If failed.
     */
    public void testProcessorInvocationMultiJVM() throws Exception {

        CacheEntryProcessor<String, String, Object> processor = new CacheEntryProcessor<String, String, Object>() {
            @Override
            public Object process(MutableEntry<String, String> e, Object... args) throws EntryProcessorException {
                new GridPortableTestClass1(); // IGNITE-3935: this no-op line breaks things
                StringBuilder b = new StringBuilder();
                for (int i = e.getKey().length() - 1; i >= 0; i--) {
                    b.append(e.getKey().charAt(i));
                }
                e.setValue(b.toString());
                return null;
            }
        };

        Ignite ignite = startGrid(0);

        // Hide path to GridPortableTestClass1 before creating other JVMs
        String classpath = System.getProperty(JAVA_CLASS_PATH);
        Collection<String> classpathItems = Lists.newArrayList(Splitter.on(File.pathSeparator).split(classpath));
        System.setProperty(JAVA_CLASS_PATH,
            Joiner.on(File.pathSeparator).join(
                Collections2.filter(classpathItems, new Predicate<String>() {
                    @Override
                    public boolean apply(String s) {
                        return !s.contains("test1-1.1.jar");
                    }
                })
            )
        );

        startGrid(1);
        startGrid(2);

        // Restore original classpath
        System.setProperty(JAVA_CLASS_PATH, classpath);

        final IgniteCache<String, String> cache = ignite.getOrCreateCache(CACHE_NAME);

        cache.put("stressed", "doesn't matter");
        cache.localEvict(Collections.singletonList("stressed"));
        ignite.close();

        Ignite igniteAgain = startGrid(0);
        IgniteCache<String, String> cacheAgain = igniteAgain.getOrCreateCache(CACHE_NAME);

        cacheAgain.invoke("stressed", StreamTransformer.from(processor));

        assert "desserts".equals(cacheAgain.get("stressed"));

        cacheAgain.destroy();
        cacheAgain.close();
        igniteAgain.close();
    }
}
