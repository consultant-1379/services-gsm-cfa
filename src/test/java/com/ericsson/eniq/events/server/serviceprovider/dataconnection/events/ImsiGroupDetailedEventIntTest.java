/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
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

import com.ericsson.eniq.events.server.dataproviders.dataconnection.ImsiGroupDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.dataconnection.events.SubscriberGroupDetailedService;

/**
 * @author eatiaro
 * @since 2011
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class ImsiGroupDetailedEventIntTest extends ServiceBaseTest {

    @Resource(name = "imsiGroupDetailedEventService")
    private SubscriberGroupDetailedService imsiGroupDetailedEventService;

    @Test
    @Parameters(source = ImsiGroupDataProvider.class)
    public void testSubscriberDetailedEventData(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, imsiGroupDetailedEventService);
    }
}
