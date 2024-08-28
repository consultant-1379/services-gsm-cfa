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

import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureAccessAreaGroupEventAnlysisDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.AccessAreaGroupSummaryService;

/**
 * @author edivana,ekumkdn
 * @since 2012
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class AccessAreaGroupEventSummaryIntegrationTest extends ServiceBaseTest {

    @Resource(name = "accessAreaGroupSummary")
    private AccessAreaGroupSummaryService accessAreaGroupSummary;

    @Test
    @Parameters(source = GSMCallFailureAccessAreaGroupEventAnlysisDataProvider.class)
    public void testAccessAreaGroupEventSummaryData(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, accessAreaGroupSummary);
    }
}
