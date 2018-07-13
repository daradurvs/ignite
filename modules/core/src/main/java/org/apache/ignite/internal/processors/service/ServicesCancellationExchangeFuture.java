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

import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;

/**
 *
 */
public class ServicesCancellationExchangeFuture extends ServicesAssignmentsExchangeFuture {
    /** */
    private final GridServiceProcessor proc;

    /** */
    private final ServicesCancellationRequestMessage msg;

    /** */
    private final AffinityTopologyVersion topVer;

    /**
     * @param proc
     * @param msg
     * @param topVer
     */
    public ServicesCancellationExchangeFuture(GridServiceProcessor proc,
        ServicesCancellationRequestMessage msg, AffinityTopologyVersion topVer) {
        super();

        this.proc = proc;
        this.msg = msg;
        this.topVer = topVer;
    }

    /** {@inheritDoc} */
    @Override public void init() {
        msg.exchId = exchId;

        assert exchId != null;

        proc.onCancellationRequest(msg, topVer);
    }
}
