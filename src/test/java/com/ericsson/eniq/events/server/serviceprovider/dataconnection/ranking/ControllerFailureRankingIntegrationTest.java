/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.serviceprovider.dataconnection.ranking;

import javax.annotation.*;
import javax.ws.rs.core.*;

import junitparams.*;

import org.junit.*;
import org.junit.runner.*;
import org.springframework.test.context.ContextConfiguration;

import com.ericsson.eniq.events.server.resources.automation.*;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.dataconnection.ranking.ControllerFailureRankingService;
import com.ericsson.eniq.events.server.dataproviders.*;

@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = {"classpath:gsm-cfa-service-context.xml"})
public class ControllerFailureRankingIntegrationTest extends ServiceBaseTest {

    @Resource(name = "controllerConnectionFailureRankingService")
    private ControllerFailureRankingService controllerConnectionFailureRankingService;

    @Test
    @Parameters(source = GSMCallFailureRankingDataProvider.class)
    public void testCallDropGetData(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, controllerConnectionFailureRankingService);
    }
}
