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


import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureTerminalSubCauseCodeAnalysisDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.cc.TerminalSubCCService;

@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
@Ignore
public class GSMCFATerminalSubCCIntegrationTest extends ServiceBaseTest {

    @Resource(name = "gsmCallFailureTerminalSubCauseCodeAnalysisService")
    private TerminalSubCCService gsmCallFailureTerminalSubCauseCodeAnalysisService;

    @Test
    @Parameters(source = GSMCallFailureTerminalSubCauseCodeAnalysisDataProvider.class)
    public void testGSMCallFailureTerminalSubCauseCodeAnalysis(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmCallFailureTerminalSubCauseCodeAnalysisService);

    }
}
