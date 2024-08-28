/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.serviceprovider.dataconnection.cc;

import javax.annotation.Resource;
import javax.ws.rs.core.MultivaluedMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import com.ericsson.eniq.events.server.dataproviders.dataconnection.IMSIGroupSuccessCauseCodeDataProvider;
import com.ericsson.eniq.events.server.dataproviders.dataconnection.IMSISuccessCauseCodeDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.dataconnection.cc.SubscriberSuccessCCService;

/**
 * @author ejoegaf
 * @since 2012
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class IMSISuccessCCIntegrationTest extends ServiceBaseTest {

    @Resource(name = "gsmDataConnectionIMSISuccessCauseCodeService")
    private SubscriberSuccessCCService gsmDataConnectionIMSISuccessCauseCodeService;

    @Test
    @Parameters(source = IMSISuccessCauseCodeDataProvider.class)
    public void testGSMDataConnectionIMSISuccessCauseCode(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmDataConnectionIMSISuccessCauseCodeService);
    }

    @Test
    @Parameters(source = IMSIGroupSuccessCauseCodeDataProvider.class)
    public void testGSMDataConnectionIMSIGroupSuccessCauseCode(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmDataConnectionIMSISuccessCauseCodeService);
    }
}
