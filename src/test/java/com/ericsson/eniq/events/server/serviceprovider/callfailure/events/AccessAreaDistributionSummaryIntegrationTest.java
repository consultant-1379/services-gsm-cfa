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

import com.ericsson.eniq.events.server.dataproviders.AccessAreaDistributionSummaryDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.AccessAreaDistributionSummaryService;

@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class AccessAreaDistributionSummaryIntegrationTest extends ServiceBaseTest {

    @Resource(name = "accessAreaDistributionSummaryService")
    private AccessAreaDistributionSummaryService cellDistributionSummaryService;

    @Test
    @Parameters(source = AccessAreaDistributionSummaryDataProvider.class)
    public void testGetData(final MultivaluedMap<String, String> requestParameters) {
        final String result = runQuery(requestParameters, cellDistributionSummaryService);
        jsonAssertUtils.assertJSONSucceeds(result);
    }

    @Test
    @Parameters(source = AccessAreaDistributionSummaryDataProvider.class)
    public void testGetDataAsCSV(final MultivaluedMap<String, String> requestParameters) {
        runQueryForCSV(requestParameters, cellDistributionSummaryService);
    }

}
