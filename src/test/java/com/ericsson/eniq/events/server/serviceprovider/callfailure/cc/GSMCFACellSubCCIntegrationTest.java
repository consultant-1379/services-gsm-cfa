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

import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureCellSubCauseCodeChartDataProvider;
import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureCellSubCauseCodeGridDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.cc.AccessAreaSubCCService;

/**
 * @author ewanggu
 * @since 2011
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
@Ignore
public class GSMCFACellSubCCIntegrationTest extends ServiceBaseTest {

    @Resource(name = "gsmCallFailureAccessAreaSubCauseCodeAnalysisService")
    private AccessAreaSubCCService gsmCallFailureAccessAreaSubCauseCodeAnalysisService;

    @Test
    @Parameters(source = GSMCallFailureCellSubCauseCodeGridDataProvider.class)
    public void testGSMCallFailureCellSubCauseCodeGrid(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmCallFailureAccessAreaSubCauseCodeAnalysisService);
    }

    @Test
    @Parameters(source = GSMCallFailureCellSubCauseCodeChartDataProvider.class)
    public void testGSMCallFailureCellSubCauseCodeChart(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmCallFailureAccessAreaSubCauseCodeAnalysisService);
    }
}
