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

import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureBscGroupEventAnlysisDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.ControllerGroupEventAnalysisService;

/**
 * @author ETHOMIT
 * @since 2012
 *
 */

@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class GSMCFABscGroupEventSummaryIntegrationTest extends ServiceBaseTest {

	@Resource(name = "gsmCallFailureBscGroupEventAnlysisSummaryService")
	private ControllerGroupEventAnalysisService gsmCallFailureBscGroupEventAnlysisSummaryService;

	@Test
	@Parameters(source = GSMCallFailureBscGroupEventAnlysisDataProvider.class)
	public void testGetDataForBscGroup(	final MultivaluedMap<String, String> requestParameters) {
		final String result = runQuery(requestParameters, gsmCallFailureBscGroupEventAnlysisSummaryService);
		jsonAssertUtils.assertJSONSucceeds(result);
	}
	
	@Test
    @Parameters(source = GSMCallFailureBscGroupEventAnlysisDataProvider.class)
    public void testGetDataBscGroupAsCSV(final MultivaluedMap<String, String> requestParameters) {
        runQueryForCSV(requestParameters, gsmCallFailureBscGroupEventAnlysisSummaryService);
    }

}
