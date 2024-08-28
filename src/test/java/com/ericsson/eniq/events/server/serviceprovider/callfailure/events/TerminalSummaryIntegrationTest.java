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

import com.ericsson.eniq.events.server.dataproviders.TerminalSummaryDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.TerminalSummaryService;

@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class TerminalSummaryIntegrationTest extends ServiceBaseTest {

    @Resource(name = "terminalSummaryService")
    private TerminalSummaryService terminalSummaryService;

    @Test
    @Parameters(source = TerminalSummaryDataProvider.class)
    public void testGetData(final MultivaluedMap<String, String> requestParameters) {
        final String result = runQuery(requestParameters, terminalSummaryService);
        jsonAssertUtils.assertJSONSucceeds(result);
    }

    @Test
    @Parameters(source = TerminalSummaryDataProvider.class)
    public void testGetDataAsCSV(final MultivaluedMap<String, String> requestParameters) {
        runQueryForCSV(requestParameters, terminalSummaryService);
    }

}
