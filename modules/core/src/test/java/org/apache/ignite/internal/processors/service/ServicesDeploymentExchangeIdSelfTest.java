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

package org.apache.ignite.internal.processors.service;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.GridTestNode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;

/**
 * Tests of {@link ServicesDeploymentExchangeId}.
 */
@RunWith(Parameterized.class)
public class ServicesDeploymentExchangeIdSelfTest {
    /** Tests discovery event. */
    private final DiscoveryEvent evt;

    /** Tests topology version. */
    private final AffinityTopologyVersion topVer;

    /** Subject under test. */
    private final ServicesDeploymentExchangeId sut;

    /**
     * @param data Tests data.
     */
    public ServicesDeploymentExchangeIdSelfTest(IgniteBiTuple<DiscoveryEvent, AffinityTopologyVersion> data) {
        this.evt = data.get1();
        this.topVer = data.get2();
        this.sut = new ServicesDeploymentExchangeId(evt, topVer);
    }

    /**
     * @return Tests data.
     */
    @Parameterized.Parameters
    public static Collection<Object[]> instancesToTest() {
        DiscoveryEvent evt = new DiscoveryEvent(
            new GridTestNode(UUID.randomUUID()), "", 10, new GridTestNode(UUID.randomUUID()));

        DiscoveryCustomEvent customEvt = new DiscoveryCustomEvent();

        customEvt.customMessage(
            new DynamicServicesChangeRequestBatchMessage(Collections.singletonList(
                DynamicServiceChangeRequest.undeploymentRequest(IgniteUuid.randomUuid())))
        );

        customEvt.eventNode(new GridTestNode(UUID.randomUUID()));

        return Arrays.asList(new Object[][] {
            {new IgniteBiTuple<>(customEvt, new AffinityTopologyVersion(ThreadLocalRandom.current().nextLong()))},
            {new IgniteBiTuple<>(evt, new AffinityTopologyVersion(ThreadLocalRandom.current().nextLong()))}});
    }

    /** */
    @Test
    public void nodeId() {
        assertEquals(evt.eventNode().id(), sut.nodeId());
    }

    /** */
    @Test
    public void eventType() {
        assertEquals(evt.type(), sut.eventType());
    }

    /** */
    @Test
    public void topologyVersion() {
        assertEquals(topVer, sut.topologyVersion());
    }

    /** */
    @Test
    public void requestId() {
        IgniteUuid reqId = evt instanceof DiscoveryCustomEvent ? ((DiscoveryCustomEvent)evt).customMessage().id() : null;

        assertEquals(reqId, sut.requestId());
    }
}