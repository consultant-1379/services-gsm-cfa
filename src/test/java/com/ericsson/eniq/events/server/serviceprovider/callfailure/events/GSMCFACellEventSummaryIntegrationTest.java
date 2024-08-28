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

import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureAccessAreaEventSummaryDataProvider;
import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureAccessAreaGroupDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.AccessAreaSummaryService;

/**
 * @author ehorpte
 * @since 2011
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class GSMCFACellEventSummaryIntegrationTest extends ServiceBaseTest {

    @Resource(name = "gsmAccessAreaCallFailureEventSummaryService")
    private AccessAreaSummaryService gsmAccessAreaCallFailureEventSummaryService;

    @Test
    @Parameters(source = GSMCallFailureAccessAreaEventSummaryDataProvider.class)
    public void testGetData(final MultivaluedMap<String, String> requestParameters) {
        final String result = runQuery(requestParameters, gsmAccessAreaCallFailureEventSummaryService);
        jsonAssertUtils.assertJSONSucceeds(result);
    }

    @Test
    @Parameters(source = GSMCallFailureAccessAreaEventSummaryDataProvider.class)
    public void testGetDataAsCSV(final MultivaluedMap<String, String> requestParameters) {
        runQueryForCSV(requestParameters, gsmAccessAreaCallFailureEventSummaryService);
    }

    @Test
    @Parameters(source = GSMCallFailureAccessAreaGroupDataProvider.class)
    public void testGetDataForCellGroup(final MultivaluedMap<String, String> requestParameters) {
        final String result = runQuery(requestParameters, gsmAccessAreaCallFailureEventSummaryService);
        jsonAssertUtils.assertJSONSucceeds(result);
    }
}
