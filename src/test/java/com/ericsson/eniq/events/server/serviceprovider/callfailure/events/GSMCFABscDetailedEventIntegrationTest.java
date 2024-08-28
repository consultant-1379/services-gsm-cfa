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

import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureBscDetailedEventAnalysisDataProvider;
import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureBscGroupDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.ControllerDetailedService;

/**
 * @author ewanggu
 * @since 2011
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class GSMCFABscDetailedEventIntegrationTest extends ServiceBaseTest {

    @Resource(name = "gsmCallFailureBscDetailedEventAnalysisService")
    private ControllerDetailedService gsmCallFailureBscDetailedEventAnalysisService;

    @Test
    @Parameters(source = GSMCallFailureBscDetailedEventAnalysisDataProvider.class)
    public void testBscCallDropData(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmCallFailureBscDetailedEventAnalysisService);
    }

    @Test
    @Parameters(source = GSMCallFailureBscGroupDataProvider.class)
    public void testCFAforBscGroup(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmCallFailureBscDetailedEventAnalysisService);
    }
}
