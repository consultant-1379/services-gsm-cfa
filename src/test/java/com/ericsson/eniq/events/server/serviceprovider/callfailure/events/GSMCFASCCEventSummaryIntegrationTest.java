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

import com.ericsson.eniq.events.server.dataproviders.GSMCFASCCSummaryEventAnalysisDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.CCSubCCSummaryService;

/**
 * @author ewanggu
 * @since 2012
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class GSMCFASCCEventSummaryIntegrationTest extends ServiceBaseTest {

    @Resource(name = "gsmCFASubCauseCodeEventSummaryService")
    private CCSubCCSummaryService gsmCFASubCauseCodeEventSummaryService;

    @Test
    @Parameters(source = GSMCFASCCSummaryEventAnalysisDataProvider.class)
    public void testGetData(final MultivaluedMap<String, String> requestParameters) {
        final String result = runQuery(requestParameters, gsmCFASubCauseCodeEventSummaryService);
        jsonAssertUtils.assertJSONSucceeds(result);
    }

    @Test
    @Parameters(source = GSMCFASCCSummaryEventAnalysisDataProvider.class)
    public void testGetDataAsCSV(final MultivaluedMap<String, String> requestParameters) {
        runQueryForCSV(requestParameters, gsmCFASubCauseCodeEventSummaryService);
    }
}
