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

import com.ericsson.eniq.events.server.dataproviders.CFAImsiEventSummaryDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.SubscriberSummaryService;

/**
 * @author eramiye
 * @since 2012
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class CFAImsiEventSummaryIntTest extends ServiceBaseTest {

    @Resource(name = "subscriberSummaryService")
    private SubscriberSummaryService gsmCallFailureImsiEventAnlysisSummaryService;

    @Test
    @Parameters(source = CFAImsiEventSummaryDataProvider.class)
    public void testGetData(final MultivaluedMap<String, String> requestParameters) {
        final String result = runQuery(requestParameters, gsmCallFailureImsiEventAnlysisSummaryService);
        jsonAssertUtils.assertJSONSucceeds(result);
    }

    @Test
    @Parameters(source = CFAImsiEventSummaryDataProvider.class)
    public void testGetDataAsCSV(final MultivaluedMap<String, String> requestParameters) {
        runQueryForCSV(requestParameters, gsmCallFailureImsiEventAnlysisSummaryService);
    }

}
