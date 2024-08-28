/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.serviceprovider.dataconnection.ranking;

import javax.annotation.Resource;
import javax.ws.rs.core.MultivaluedMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.dataconnection.ranking.SubscriberDataVolumeRankingService;
import com.ericsson.eniq.events.server.dataproviders.dataconnection.RankingDataProvider;

/**
 * @author ejoegaf
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = {"classpath:gsm-cfa-service-context.xml"})
public class ImsiDataVolumeRankingIntegrationTest extends ServiceBaseTest {

    @Resource(name = "gsmDataConnectionSubscriberDataVolumeRankingService")
    private SubscriberDataVolumeRankingService gsmDataConnectionSubscriberDataVolumeRankingService;

    @Test
    @Parameters(source = RankingDataProvider.class)
    public void testCallDropGetData(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, gsmDataConnectionSubscriberDataVolumeRankingService);
    }
}
