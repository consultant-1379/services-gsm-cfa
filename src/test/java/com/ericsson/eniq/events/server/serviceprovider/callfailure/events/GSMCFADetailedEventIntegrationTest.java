/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.serviceprovider.callfailure.events;

import javax.annotation.Resource;
import javax.ws.rs.core.MultivaluedMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureCallDropsDetailedEventAnalysisDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.CCSubCCDetailedService;

/**
 * @author ewanggu
 * @since 2011
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class GSMCFADetailedEventIntegrationTest extends ServiceBaseTest {

    @Resource(name = "gsmCallFailureCallDropsDetailedEventAnalysisService")
    private CCSubCCDetailedService gsmCallFailureCallDropsDetailedEventAnalysisService;

    @Test
    @Parameters(source = GSMCallFailureCallDropsDetailedEventAnalysisDataProvider.class)
    public void testCauseCodeCallDropData(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmCallFailureCallDropsDetailedEventAnalysisService);
    }
}
