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

import com.ericsson.eniq.events.server.dataproviders.dataconnection.IMSICauseCodeListDataProvider;
import com.ericsson.eniq.events.server.dataproviders.dataconnection.IMSIGroupCauseCodeListDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.dataconnection.cc.SubscriberCCListService;

/**
 * @author ejoegaf
 * @since 2012
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class CCListIntegrationTest extends ServiceBaseTest {

    @Resource(name = "gsmDataConnectionIMSICauseCodeListService")
    private SubscriberCCListService gsmDataConnectionIMSICauseCodeListService;

    @Test
    @Parameters(source = IMSICauseCodeListDataProvider.class)
    public void testGSMDataConnectionIMSICauseCodeList(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmDataConnectionIMSICauseCodeListService);
    }

    @Test
    @Parameters(source = IMSIGroupCauseCodeListDataProvider.class)
    public void testGSMDataConnectionIMSIGroupCauseCodeList(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmDataConnectionIMSICauseCodeListService);
    }
}
