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

import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryMarshallerSelfTest;

/**
 * Compression tests
 */
public class DataCompressionTest extends BinaryMarshallerSelfTest {

    /**
     * @throws Exception If failed.
     */
    public void testObjectCompression() throws Exception {
        SubjectUnderTest sut = new SubjectUnderTest("ABCDEFGHIJKLMNOPQRSTUVWXYZ");

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

    /** Test class*/
    private static class SubjectUnderTest {
        @BinaryCompression
        private String data;

        public SubjectUnderTest(String data) {
            this.data = data;
        }

        public String getData() {
            return data;
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            SubjectUnderTest test = (SubjectUnderTest)o;

            return data != null ? data.equals(test.getData()) : test.getData() == null;
        }
    }
}