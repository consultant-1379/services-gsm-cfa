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

import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureAccessAreaDetailedEventAnalysisDataProvider;
import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureAccessAreaGroupDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.AccessAreaDetailedService;

/**
 * @author ewanggu
 * @since 2011
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class GSMCFACellDetailedIntegrationTest extends ServiceBaseTest {

    @Resource(name = "gsmCallFailureAccessAreaDetailedEventAnalysisService")
    private AccessAreaDetailedService gsmCallFailureAccessAreaDetailedEventAnalysisService;

    @Test
    @Parameters(source = GSMCallFailureAccessAreaDetailedEventAnalysisDataProvider.class)
    public void testAccessAreaCallDropData(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmCallFailureAccessAreaDetailedEventAnalysisService);
    }

    @Test
    @Parameters(source = GSMCallFailureAccessAreaGroupDataProvider.class)
    public void testAccessAreaGroupCallDropData(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmCallFailureAccessAreaDetailedEventAnalysisService);
    }
}
