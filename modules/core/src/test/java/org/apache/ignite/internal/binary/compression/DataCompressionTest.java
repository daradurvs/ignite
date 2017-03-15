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

package org.apache.ignite.internal.binary.compression;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryCachingMetadataHandler;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryMarshallerSelfTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.logger.NullLogger;

/**
 * Compression tests
 */
public class DataCompressionTest extends BinaryMarshallerSelfTest {
    /** Test string line. */
    String line = "абвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    /**
     * @throws Exception If failed.
     */
    public void testCacheCompression() throws Exception {
        String gridName = getTestGridName();
        IgniteConfiguration cfg = getConfiguration(gridName);
//        cfg.setDefaultCompression(true);
        cfg.setMarshaller(binaryMarshaller());
//        cfg.setDefaultCompressionType(CompressionType.GZIP);

        try (Ignite ignite = startGrid(gridName, cfg)) {

//            IgniteCache<Integer, String> cache = ignite.getOrCreateCache("test");
            IgniteCache<Integer, SimpleObject> cache = ignite.getOrCreateCache("test");

//            cache.put(1, "abc");

            cache.put(1, new SimpleObject("simpleObjectConstructor"));

            IgniteCache<BinaryObject, BinaryObject> binaryCache = cache.withKeepBinary();

//            assertEquals(cache.get(1), "abc");

        }
    }

    public class SimpleObject {
        private String simpleField;

        public SimpleObject(String simpleField) {
            this.simpleField = simpleField;
        }

        public String getSimpleField() {
            return simpleField;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDefaultCompression() throws Exception {
        IgniteConfiguration igniteConfiguration = getConfiguration(getTestGridName());
        igniteConfiguration.setDefaultCompression(true);

        BinaryContext ctx = new BinaryContext(BinaryCachingMetadataHandler.create(), igniteConfiguration, new NullLogger());
        BinaryMarshaller marshaller = binaryMarshaller();
        IgniteUtils.invoke(BinaryMarshaller.class, marshaller, "setBinaryContext", ctx, igniteConfiguration);

        assertEquals(line, marshalUnmarshal(line, marshaller));

        byte[] compressed = marshaller.marshal(line);
        assertEquals(line, marshaller.unmarshal(compressed, Thread.currentThread().getContextClassLoader()));
    }

    /**
     * @throws Exception If failed.
     */
    public void testObjectCompression() throws Exception {
        SubjectUnderTest sut = new SubjectUnderTest(line);

        BinaryMarshaller marshaller = binaryMarshaller();

        byte[] sutBytes = marshaller.marshal(sut);
        SubjectUnderTest unmSut = marshaller.unmarshal(sutBytes, null);

        assertEquals(sut, unmSut);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimitiveMarshalling() throws Exception {
        int sut = 12345;
        int unmSut = marshalUnmarshal(sut);
        assertEquals(sut, unmSut);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStringMarshalling() throws Exception {
        String sut = "12345";
        String unmSut = marshalUnmarshal(sut);
        assertEquals(sut, unmSut);
    }

    /**
     * @throws Exception If failed.
     */
    private <T> T marshalUnmarshal(T data) throws Exception {
        BinaryMarshaller marshaller = binaryMarshaller();
        byte[] bytes = marshaller.marshal(data);
        return marshaller.unmarshal(bytes, null);
    }

    /** Test class. */
    private static class SubjectUnderTest {
        @BinaryCompression
        private String data_default;

        @BinaryCompression(type = CompressionType.GZIP)
        private String data_gzip;

        @BinaryCompression(type = CompressionType.DEFLATE)
        private String data_deflate;

        private SubjectUnderTest(String data) {
            this.data_default = data;
            this.data_gzip = data;
            this.data_deflate = data;
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            SubjectUnderTest test = (SubjectUnderTest)o;

            if (data_default != null ? !data_default.equals(test.data_default) : test.data_default != null)
                return false;
            if (data_gzip != null ? !data_gzip.equals(test.data_gzip) : test.data_gzip != null)
                return false;
            return data_deflate != null ? data_deflate.equals(test.data_deflate) : test.data_deflate == null;

        }

        @Override public int hashCode() {
            int result = data_default != null ? data_default.hashCode() : 0;
            result = 31 * result + (data_gzip != null ? data_gzip.hashCode() : 0);
            result = 31 * result + (data_deflate != null ? data_deflate.hashCode() : 0);
            return result;
        }
    }
}