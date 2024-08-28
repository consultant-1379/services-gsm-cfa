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

import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureBscEventSummaryDataProvider;
import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureBscGroupDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.ControllerSummaryService;

@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class GSMCFABscEventSummaryIntegrationTest extends ServiceBaseTest {

    @Resource(name = "gsmCallFailureBscEventAnlysisSummaryService")
    private ControllerSummaryService gsmCallFailureBscEventAnlysisSummaryService;

    @Test
    @Parameters(source = GSMCallFailureBscEventSummaryDataProvider.class)
    public void testGetData(final MultivaluedMap<String, String> requestParameters) {
        final String result = runQuery(requestParameters, gsmCallFailureBscEventAnlysisSummaryService);
        jsonAssertUtils.assertJSONSucceeds(result);
    }

    @Test
    @Parameters(source = GSMCallFailureBscEventSummaryDataProvider.class)
    public void testGetDataAsCSV(final MultivaluedMap<String, String> requestParameters) {
        runQueryForCSV(requestParameters, gsmCallFailureBscEventAnlysisSummaryService);
    }

    @Test
    @Parameters(source = GSMCallFailureBscGroupDataProvider.class)
    public void testGetDataForBscGroup(final MultivaluedMap<String, String> requestParameters) {
        final String result = runQuery(requestParameters, gsmCallFailureBscEventAnlysisSummaryService);
        jsonAssertUtils.assertJSONSucceeds(result);
    }

}
