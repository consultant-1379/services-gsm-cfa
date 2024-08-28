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

import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureTerminalDetailedEventAnalysisDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.TerminalDetailedService;

/**
 * @author ejoegaf
 * @since 2011
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class GSMCFATerminalDetailedEventIntTest extends ServiceBaseTest {

    @Resource(name = "gsmCallFailureTerminalDetailedEventAnalysisService")
    private TerminalDetailedService gsmCallFailureTerminalDetailedEventAnalysisService;

    @Test
    @Parameters(source = GSMCallFailureTerminalDetailedEventAnalysisDataProvider.class)
    public void testAccessAreaCallDropData(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmCallFailureTerminalDetailedEventAnalysisService);
    }
}
