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

import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureSubscriberCallSetupDetailedEventAnalysisDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.SubscriberCallSetupFailureDetailedService;

@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class GSMCFACallSetupFailureImsiDetailedEventIntTest extends ServiceBaseTest {

    @Resource(name = "gsmCallFailureSubscriberCallSetupDetailedEventAnalysisService")
    private SubscriberCallSetupFailureDetailedService gsmCallFailureSubscriberCallSetupDetailedEventAnalysisService;

    @Test
    @Parameters(source = GSMCallFailureSubscriberCallSetupDetailedEventAnalysisDataProvider.class)
    public void testIMSICallFailureData(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmCallFailureSubscriberCallSetupDetailedEventAnalysisService);
    }
}
