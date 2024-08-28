package com.ericsson.eniq.events.server.serviceprovider.callfailure.roaming;

import javax.annotation.Resource;
import javax.ws.rs.core.MultivaluedMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import com.ericsson.eniq.events.server.dataproviders.roaming.OperatorDetailRoamingDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.roaming.OperatorRoamingDetailService;

/**
 * @author elasabu
 * @since 2012
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class OperatorRoamingDetailServiceIntegrationTest extends ServiceBaseTest {

    @Resource(name = "roamingByOperatorDetailService")
    private OperatorRoamingDetailService roamingByOperatorDetailService;

    @Test
    @Parameters(source = OperatorDetailRoamingDataProvider.class)
    public void testGetDataByOperator(final MultivaluedMap<String, String> requestParameters) {
        final String result = runQuery(requestParameters, roamingByOperatorDetailService);
        jsonAssertUtils.assertJSONSucceeds(result);
    }

}
