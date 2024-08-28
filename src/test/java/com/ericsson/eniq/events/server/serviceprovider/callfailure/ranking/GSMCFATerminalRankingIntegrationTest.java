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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.ranking.TerminalRankingService;
import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureRankingDataProvider;

/**
 * @author ejoegaf
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = {"classpath:gsm-cfa-service-context.xml"})
public class GSMCFATerminalRankingIntegrationTest extends ServiceBaseTest {

    @Resource(name = "gsmCallFailureTerminalRankingService")
    private TerminalRankingService gsmCallFailureTerminalRankingService;

    @Test
    @Parameters(source = GSMCallFailureRankingDataProvider.class)
    public void testCallDropGetData(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmCallFailureTerminalRankingService);
    }
}
