/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
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

import com.ericsson.eniq.events.server.dataproviders.CFAImsiGroupEventSummaryDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.SubscriberGroupBreakdownService;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.SubscriberSummaryService;

/**
 * @author eramiye
 * @since 2012
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class CFAImsiGroupEventSummaryIntTest extends ServiceBaseTest {

    @Resource(name = "subscriberSummaryService")
    private SubscriberSummaryService gsmCallFailureImsiEventAnlysisSummaryService;

    @Resource(name = "subscriberGroupBreakdownService")
    private SubscriberGroupBreakdownService gsmSubscriberGroupSummaryBreakdownService;

    @Test
    @Parameters(source = CFAImsiGroupEventSummaryDataProvider.class)
    public void testGetData(final MultivaluedMap<String, String> requestParameters) {
        final String result = runQuery(requestParameters, gsmCallFailureImsiEventAnlysisSummaryService);
        jsonAssertUtils.assertJSONSucceeds(result);
    }

    @Test
    @Parameters(source = CFAImsiGroupEventSummaryDataProvider.class)
    public void testGetDataAsCSV(final MultivaluedMap<String, String> requestParameters) {
        runQueryForCSV(requestParameters, gsmCallFailureImsiEventAnlysisSummaryService);
    }

    @Test
    @Parameters(source = CFAImsiGroupEventSummaryDataProvider.class)
    public void testGetDataForSummaryBreakdown(final MultivaluedMap<String, String> requestParameters) {
        final String result = runQuery(requestParameters, gsmSubscriberGroupSummaryBreakdownService);
        jsonAssertUtils.assertJSONSucceeds(result);
    }

}
