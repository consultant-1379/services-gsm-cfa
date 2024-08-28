/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.serviceprovider.callfailure.ranking;

import javax.annotation.Resource;
import javax.ws.rs.core.MultivaluedMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.ranking.ControllerRankingService;
import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureRankingDataProvider;

@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = {"classpath:gsm-cfa-service-context.xml"})
public class GSMControllerCFARankingIntegrationTest extends ServiceBaseTest {

    @Resource(name = "gsmControllerCFARankingService")
    private ControllerRankingService gsmControllerCFARankingService;

    @Test
    @Parameters(source = GSMCallFailureRankingDataProvider.class)
    public void testCallDropGetData(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmControllerCFARankingService);
    }
}
