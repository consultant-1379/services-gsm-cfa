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

import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureControllerSubCauseCodeAnalysisChartDataProvider;
import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureControllerSubCauseCodeAnalysisDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.cc.ControllerSubCCService;

/**
 * @author ejoegaf
 * @since 2011
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
@Ignore
public class GSMCFABscSubCCIntegrationTest extends ServiceBaseTest {

    @Resource(name = "gsmCallFailureControllerSubCauseCodeAnalysisService")
    private ControllerSubCCService gsmCallFailureControllerSubCauseCodeAnalysisService;

    @Test
    @Parameters(source = GSMCallFailureControllerSubCauseCodeAnalysisDataProvider.class)
    public void GSMCallFailureControllerSubCauseCodeAnalysis(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmCallFailureControllerSubCauseCodeAnalysisService);
    }

    @Test
    @Parameters(source = GSMCallFailureControllerSubCauseCodeAnalysisChartDataProvider.class)
    public void GSMCallFailureControllerSubCauseCodeChartAnalysis(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmCallFailureControllerSubCauseCodeAnalysisService);
    }
}
