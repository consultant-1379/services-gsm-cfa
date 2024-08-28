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

import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureAccessAreaCauseCodeListDataProvider;
import com.ericsson.eniq.events.server.dataproviders.GSMCallFailureControllerCauseCodeListDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.cc.AccessAreaCCListService;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.cc.ControllerCCListService;

/**
 * @author ewanggu
 * @since 2011
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
@Ignore
public class GSMCFACCListIntegrationTest extends ServiceBaseTest {

    @Resource(name = "gsmCallFailureAccessAreaCauseCodeListService")
    private AccessAreaCCListService gsmCallFailureAccessAreaCauseCodeListService;

    @Resource(name = "gsmCallFailureControllerCauseCodeListService")
    private ControllerCCListService gsmCallFailureControllerCauseCodeListService;

    @Test
    @Parameters(source = GSMCallFailureControllerCauseCodeListDataProvider.class)
    public void testGSMCallFailureControllerCauseCodeList(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmCallFailureControllerCauseCodeListService);
    }

    @Test
    @Parameters(source = GSMCallFailureAccessAreaCauseCodeListDataProvider.class)
    public void testGSMCallFailureAccessAreaCauseCodeList(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmCallFailureAccessAreaCauseCodeListService);
    }
}
