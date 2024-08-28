/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.serviceprovider.callfailure.roaming;

import javax.annotation.Resource;
import javax.ws.rs.core.MultivaluedMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import com.ericsson.eniq.events.server.dataproviders.roaming.RoamingDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.roaming.CountryCallFailureSummaryService;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.roaming.CountryRoamingSummaryService;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.roaming.OperatorRoamingSummaryService;

@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class RoamingSummaryIntegrationTest extends ServiceBaseTest {

    @Resource(name = "roamingByOperatorService")
    private OperatorRoamingSummaryService roamingByOperatorService;

    @Resource(name = "roamingByCountryService")
    private CountryRoamingSummaryService roamingByCountryService;

    @Resource(name = "countryCallFailureSummaryService")
    private CountryCallFailureSummaryService countryCallFailureSummaryService;

    @Test
    @Parameters(source = RoamingDataProvider.class)
    public void testGetDataByOperator(final MultivaluedMap<String, String> requestParameters) {
        final String result = runQuery(requestParameters, roamingByOperatorService);
        jsonAssertUtils.assertJSONSucceeds(result);
    }

    @Test
    @Parameters(source = RoamingDataProvider.class)
    public void testGetDataByCountry(final MultivaluedMap<String, String> requestParameters) {
        final String result = runQuery(requestParameters, roamingByCountryService);
        jsonAssertUtils.assertJSONSucceeds(result);
    }

    @Test
    @Parameters(source = RoamingDataProvider.class)
    public void testGetDataByCallFailureSummaryOfCountry(final MultivaluedMap<String, String> requestParameters) {
        final String result = runQuery(requestParameters, countryCallFailureSummaryService);
        jsonAssertUtils.assertJSONSucceeds(result);
    }
}
