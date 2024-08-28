package com.ericsson.eniq.events.server.serviceprovider.dataconnection.ranking;

import javax.annotation.Resource;
import javax.ws.rs.core.MultivaluedMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import com.ericsson.eniq.events.server.dataproviders.dataconnection.RankingDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.dataconnection.ranking.SubscriberFailureRankingService;

/**
 * 
 * @author eramiye
 * @since 2012
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class ImsiFailureRankingIntegrationTest extends ServiceBaseTest {

    @Resource(name = "connectionFailureByImsiRankingService")
    private SubscriberFailureRankingService connectionFailureByImsiRankingService;

    @Test
    @Parameters(source = RankingDataProvider.class)
    public void testCallDropGetData(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, connectionFailureByImsiRankingService);
    }

}
