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

package org.apache.ignite.cache.spring;

import java.io.Serializable;
import java.util.concurrent.Callable;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

/**
 * An invocable function that allows applications to perform compound operations
 * on a {@link javax.cache.Cache.Entry} atomically, according the defined
 * consistency of a {@link Cache}.
 *
 * @param <T> The type of the return value.
 */
public class ValueLoaderEntryProcessor<T> implements EntryProcessor<Object, Object, T>, Serializable {
    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Object process(MutableEntry entry, Object... args) throws EntryProcessorException {
        Callable<T> valLdr = (Callable<T>)args[0];

        if (entry.exists())
            return (T)entry.getValue();
        else {
            T val;

            try {
                val = valLdr.call();
            }
            catch (Exception e) {
                throw new EntryProcessorException("Value loader '" + valLdr + "' failed " +
                    "to compute  value for key '" + entry.getKey() + "'", e);
            }

            if (val != null)
                entry.setValue(val);

            return val;
        }
    }
}
