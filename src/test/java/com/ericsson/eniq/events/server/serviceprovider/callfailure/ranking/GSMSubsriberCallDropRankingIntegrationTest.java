/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.serviceprovider.callfailure.ranking;

import javax.annotation.Resource;
import javax.ws.rs.core.MultivaluedMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.ranking.SubscriberRankingService;
import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureRankingDataProvider;

/**
 * @author ewanggu
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = {"classpath:gsm-cfa-service-context.xml"})
public class GSMSubsriberCallDropRankingIntegrationTest extends ServiceBaseTest {

    @Resource(name = "gsmCallFailureSubscriberCallDropRankingService")
    private SubscriberRankingService gsmCallFailureSubscriberCallDropRankingService;

    @Test
    @Parameters(source = GSMCallFailureRankingDataProvider.class)
    public void testCallDropGetData(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmCallFailureSubscriberCallDropRankingService);
    }
}
