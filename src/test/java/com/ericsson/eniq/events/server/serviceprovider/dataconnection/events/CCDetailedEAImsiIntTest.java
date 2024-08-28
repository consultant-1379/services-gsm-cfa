/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.serviceprovider.dataconnection.events;

import javax.annotation.Resource;
import javax.ws.rs.core.MultivaluedMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import com.ericsson.eniq.events.server.dataproviders.dataconnection.CCDetailedEAImsiDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.dataconnection.events.SubscriberCCDetailedService;

/**
 * @author eramiye
 * @since 2012
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class CCDetailedEAImsiIntTest extends ServiceBaseTest {

    @Resource(name = "causeCodeDetailedEventAnalysisImsi")
    private SubscriberCCDetailedService causeCodeDetailedEventAnalysisImsi;

    @Test
    @Parameters(source = CCDetailedEAImsiDataProvider.class)
    public void testCCDetailedEventImsiData(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, causeCodeDetailedEventAnalysisImsi);
    }
}
