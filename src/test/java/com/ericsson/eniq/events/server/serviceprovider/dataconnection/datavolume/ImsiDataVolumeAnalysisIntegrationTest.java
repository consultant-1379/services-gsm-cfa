/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.serviceprovider.dataconnection.datavolume;

import javax.annotation.Resource;
import javax.ws.rs.core.MultivaluedMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import com.ericsson.eniq.events.server.dataproviders.dataconnection.SubscriberDataVolumeAnalysisDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.dataconnection.datavolume.SubscriberDataVolumeService;

/**
 * @author ethomit
 * @since 2012
 * 
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class ImsiDataVolumeAnalysisIntegrationTest extends ServiceBaseTest {

    @Resource(name = "subscriberDataVolumeAnalysisService")
    private SubscriberDataVolumeService subscriberDataVolumeAnalysisService;

    @Test
    @Parameters(source = SubscriberDataVolumeAnalysisDataProvider.class)
    public void testSubscriberDataVolumeData(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, subscriberDataVolumeAnalysisService);
    }
}
