/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2014
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.eniq.events.server.serviceprovider.dataconnection.events;

import javax.annotation.Resource;
import javax.ws.rs.core.MultivaluedMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import com.ericsson.eniq.events.server.dataproviders.dataconnection.SubscriberDetailedEventAnalysisDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.dataconnection.events.DataConnectionSubscriberDetailedService;

/**
 * @since 2011
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class ImsiDetailedEventIntegrationTest extends ServiceBaseTest {

    @Resource(name = "subscriberDetailedEventAnalysisService")
    private DataConnectionSubscriberDetailedService subscriberDetailedEventAnalysisService;

    @Test
    @Parameters(source = SubscriberDetailedEventAnalysisDataProvider.class)
    public void testSubscriberDetailedEventData(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, subscriberDetailedEventAnalysisService);
    }
}
