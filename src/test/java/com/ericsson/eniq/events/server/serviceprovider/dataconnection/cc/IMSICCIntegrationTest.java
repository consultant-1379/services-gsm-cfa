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

import com.ericsson.eniq.events.server.dataproviders.dataconnection.IMSICauseCodeChartDataProvider;
import com.ericsson.eniq.events.server.dataproviders.dataconnection.IMSIGroupCauseCodeChartDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.dataconnection.cc.SubscriberCCService;

/**
 * @author ejoegaf
 * @since 2012
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class IMSICCIntegrationTest extends ServiceBaseTest {

    @Resource(name = "gsmDataConnectionIMSICauseCodeService")
    private SubscriberCCService gsmDataConnectionIMSICauseCodeService;

    @Test
    @Parameters(source = IMSICauseCodeChartDataProvider.class)
    public void testGSMDataConnectionIMSICauseCode(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmDataConnectionIMSICauseCodeService);
    }

    @Test
    @Parameters(source = IMSIGroupCauseCodeChartDataProvider.class)
    public void testGSMDataConnectionIMSIGroupCauseCode(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmDataConnectionIMSICauseCodeService);
    }
}
