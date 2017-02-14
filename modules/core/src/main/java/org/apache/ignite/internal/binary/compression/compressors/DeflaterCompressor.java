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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.DataFormatException;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import org.jetbrains.annotations.NotNull;

/**
 * TODO: description
 */
public class DeflaterCompressor implements Compressor {
    /** {@inheritDoc} */
    @Override public byte[] compress(@NotNull byte[] bytes) throws IOException {
        try (
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            OutputStream out = new DeflaterOutputStream(baos)
        ) {
            out.write(bytes);
            out.flush();
            return baos.toByteArray();
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] decompress(@NotNull byte[] bytes) throws IOException {
        Inflater decompressor = new Inflater();
        decompressor.setInput(bytes);

        try (
            ByteArrayOutputStream baos = new ByteArrayOutputStream()
        ) {
            byte[] buffer = new byte[1024];
            while (!decompressor.finished()) {
                int length = decompressor.inflate(buffer);
                baos.write(buffer, 0, length);
            }
            return baos.toByteArray();
        }
        catch (DataFormatException ignored) {
            return new byte[0];
        }
    }
}
