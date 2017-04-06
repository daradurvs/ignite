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

package org.apache.ignite.stream.akka;

import akka.Done;
import akka.stream.javadsl.Sink;
import java.util.concurrent.CompletionStage;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.stream.StreamAdapter;

public class IgniteAkkaStreamer<K, V> extends StreamAdapter<Object, K, V> {
    /** Logger. */
    protected IgniteLogger log;

    public IgniteAkkaStreamer() {
    }

    /**
     * Create Sink Akka streamer.
     */
    public Sink sink() {
        final Sink<Integer, CompletionStage<Done>> sink = Sink.foreach(e -> {
            addMessage(e);
        });

        return sink;
    }
}
