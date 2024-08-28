/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.serviceprovider.callfailure.cc;

import javax.annotation.Resource;
import javax.ws.rs.core.MultivaluedMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureTerminalCauseCodeAnalysisDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.cc.TerminalCCService;

@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
@Ignore
public class GSMCFATerminalCCIntegrationTest extends ServiceBaseTest {

    @Resource(name = "gsmCallFailureTerminalCauseCodeAnalysisService")
    private TerminalCCService gsmCallFailureTerminalCauseCodeAnalysisService;

    @Test
    @Parameters(source = GSMCallFailureTerminalCauseCodeAnalysisDataProvider.class)
    public void testGSMCallFailureControllerCauseCodeAnalysis(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmCallFailureTerminalCauseCodeAnalysisService);
    }

}
