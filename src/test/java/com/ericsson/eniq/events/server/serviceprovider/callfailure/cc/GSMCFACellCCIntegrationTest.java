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

import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureCellCauseCodeChartDataProvider;
import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureCellCauseCodeGridDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.cc.AccessAreaCCService;

/**
 * @author ewanggu
 * @since 2011
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
@Ignore
public class GSMCFACellCCIntegrationTest extends ServiceBaseTest {

    @Resource(name = "gsmCallFailureAccessAreaCauseCodeAnalysisService")
    private AccessAreaCCService gsmCallFailureAccessAreaCauseCodeAnalysisService;

    @Test
    @Parameters(source = GSMCallFailureCellCauseCodeGridDataProvider.class)
    public void testGSMCallFailureCellCauseCodeGrid(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmCallFailureAccessAreaCauseCodeAnalysisService);
    }

    @Test
    @Parameters(source = GSMCallFailureCellCauseCodeChartDataProvider.class)
    public void testGSMCallFailureCellCauseCodeChart(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmCallFailureAccessAreaCauseCodeAnalysisService);
    }
}
