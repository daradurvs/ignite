package org.apache.ignite.internal.processors.service;

import java.util.Map;
import org.apache.ignite.lang.IgniteUuid;

public class ServicesExchangeActions {
    Map<IgniteUuid, ServiceInfo> srvcsToDeploy;

    Map<IgniteUuid, ServiceInfo> srvcsToUndeploy;

    boolean deactivate;
}
