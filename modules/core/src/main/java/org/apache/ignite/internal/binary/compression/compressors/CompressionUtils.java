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

package org.apache.ignite.internal.binary.compression.compressors;

import java.io.IOException;
import java.util.Map;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.compression.CompressionType;

/**
 * Utilities methods for working with compression.
 */
public class CompressionUtils {

    /** */
    public static byte[] compress(BinaryContext context, CompressionType type,
        byte[] bytes) throws BinaryObjectException {
        Map<CompressionType, Compressor> compressorsSelector = context.configuration().getCompressorsSelector();

        return compress(compressorsSelector.get(type), bytes);
    }

    /** */
    public static byte[] compress(Compressor compressor, byte[] bytes) throws BinaryObjectException {
        try {
            return compressor.compress(bytes);
        }
        catch (IOException e) {
            throw new BinaryObjectException("Failed to compress bytes", e);
        }
    }

    /** */
    public static byte[] decompress(BinaryContext context, CompressionType type,
        byte[] bytes) throws BinaryObjectException {
        Map<CompressionType, Compressor> compressorsSelector = context.configuration().getCompressorsSelector();

        return decompress(compressorsSelector.get(type), bytes);
    }

    /** */
    public static byte[] decompress(Compressor compressor, byte[] bytes) throws BinaryObjectException {
        try {
            return compressor.decompress(bytes);
        }
        catch (IOException e) {
            throw new BinaryObjectException("Failed to decompress bytes", e);
        }
    }

}
