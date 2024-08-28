/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.serviceprovider.callfailure.cc;

import javax.annotation.Resource;
import javax.ws.rs.core.MultivaluedMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureControllerCauseCodeAnalysisChartDataProvider;
import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureControllerCauseCodeAnalysisDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.cc.ControllerCCService;

/**
 * @author ejoegaf
 * @since 2011
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
@Ignore
public class GSMCFABscCCIntegrationTest extends ServiceBaseTest {

    @Resource(name = "gsmCallFailureControllerCauseCodeAnalysisService")
    private ControllerCCService gsmCallFailureControllerCauseCodeAnalysisService;

    @Test
    @Parameters(source = GSMCallFailureControllerCauseCodeAnalysisDataProvider.class)
    public void testGSMCallFailureControllerCauseCodeAnalysis(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmCallFailureControllerCauseCodeAnalysisService);
    }

    @Test
    @Parameters(source = GSMCallFailureControllerCauseCodeAnalysisChartDataProvider.class)
    public void testGSMCallFailureControllerCauseCodeChartAnalysis(
            final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmCallFailureControllerCauseCodeAnalysisService);
    }
}
