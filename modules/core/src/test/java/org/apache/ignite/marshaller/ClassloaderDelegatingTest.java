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

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.stream.StreamTransformer;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.marshaller.ClassloaderDelegatingTest2.JAVA_CLASS_PATH;

/** */
public class ClassloaderDelegatingTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        ClassLoader ldr = StreamingCacheTestEntryProcessor.class.getClassLoader();

        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /**
     * @throws Exception If failed.
     */
    public void testClasspath() throws Exception {
        String classpath = System.getProperty(JAVA_CLASS_PATH);
        System.out.println(classpath);
        assertTrue(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassloadingByStreamer() throws Exception {
        IgniteConfiguration cfg = getConfiguration();
        cfg.setClientMode(true);

        try (Ignite ignite = startGrid("test", cfg)) {
            IgniteCache<String, Long> cache = ignite.getOrCreateCache("mycache");

            try (IgniteDataStreamer<String, Long> streamer = ignite.dataStreamer(cache.getName())) {
                streamer.allowOverwrite(true);
                streamer.receiver(StreamTransformer.from(new StreamingCacheTestEntryProcessor()));
                streamer.addData("word", 1L);
            }
        }
    }

    /** */
    private static class StreamingCacheTestEntryProcessor implements CacheEntryProcessor<String, Long, Object> {
        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<String, Long> e, Object... arg) throws EntryProcessorException {
            Long val = e.getValue();
            e.setValue(val == null ? 1L : val + 1);

            return null;
        }
    }
}
