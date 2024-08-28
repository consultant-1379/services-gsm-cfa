/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
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

import com.ericsson.eniq.events.server.dataproviders.dataconnection.IMSISubCauseCodeChartDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.dataconnection.cc.SubscriberSCCService;

/**
 * @author ejoegaf
 * @since 2012
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class IMSISubCCIntegrationTest extends ServiceBaseTest {

    @Resource(name = "gsmDataConnectionIMSISubCauseCodeService")
    private SubscriberSCCService gsmDataConnectionIMSISubCauseCodeService;

    @Test
    @Parameters(source = IMSISubCauseCodeChartDataProvider.class)
    public void testGSMDataConnectionIMSICauseCode(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmDataConnectionIMSISubCauseCodeService);
    }

}
