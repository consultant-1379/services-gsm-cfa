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

import com.ericsson.eniq.events.server.dataproviders.GSMDetailedEventCFAByBSCAndSCCDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.ControllerSubCCDetailedService;

/**
 * @author eatiaro
 * @since 2011
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class GSMCFASccDetailedEventIntegrationTest extends ServiceBaseTest {

    @Resource(name = "gsmCallFailureControllerSubCauseCodeDetailedEventAnalysisService")
    private ControllerSubCCDetailedService gsmCallFailureControllerSubCauseCodeDetailedEventAnalysisService;

    @Test
    @Parameters(source = GSMDetailedEventCFAByBSCAndSCCDataProvider.class)
    public void GSMCallFailureControllerSubCauseCodeDetailedAnalysis(
            final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmCallFailureControllerSubCauseCodeDetailedEventAnalysisService);
    }
}
