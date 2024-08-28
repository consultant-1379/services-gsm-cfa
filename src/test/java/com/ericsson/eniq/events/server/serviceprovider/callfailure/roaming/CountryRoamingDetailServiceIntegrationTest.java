package com.ericsson.eniq.events.server.serviceprovider.callfailure.roaming;

import javax.annotation.Resource;
import javax.ws.rs.core.MultivaluedMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import com.ericsson.eniq.events.server.dataproviders.roaming.DetailRoamingDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.roaming.CountryRoamingDetailService;

@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class CountryRoamingDetailServiceIntegrationTest extends ServiceBaseTest {

    @Resource(name = "roamingByCountryDetailService")
    private CountryRoamingDetailService roamingByCountryDetailService;

    @Test
    @Parameters(source = DetailRoamingDataProvider.class)
    public void testGetDataByCountry(final MultivaluedMap<String, String> requestParameters) {
        final String result = runQuery(requestParameters, roamingByCountryDetailService);
        jsonAssertUtils.assertJSONSucceeds(result);
    }

}
