package com.ericsson.eniq.events.server.serviceprovider.callfailure.cc;

import javax.annotation.Resource;
import javax.ws.rs.core.MultivaluedMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import com.ericsson.eniq.events.server.dataproviders.AccessAreaGroupBreakdownServiceDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.cc.AccessAreaGroupBreakdownService;


/**
 * @author ekumjay
 * @since 2012
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class AccessAreaGroupBreakdownServiceIntegrationTest extends ServiceBaseTest {

    @Resource(name = "gsmAccessAreaGroupBreakdownService")
    private AccessAreaGroupBreakdownService gsmAccessAreaGroupBreakdownService;

    @Test
    @Parameters(source = AccessAreaGroupBreakdownServiceDataProvider.class)
    public void testGetDataForBscGroup(final MultivaluedMap<String, String> requestParameters) {
        final String result = runQuery(requestParameters, gsmAccessAreaGroupBreakdownService);
        jsonAssertUtils.assertJSONSucceeds(result);
    }

    @Test
    @Parameters(source = AccessAreaGroupBreakdownServiceDataProvider.class)
    public void testGetDataBscGroupAsCSV(final MultivaluedMap<String, String> requestParameters) {
        runQueryForCSV(requestParameters, gsmAccessAreaGroupBreakdownService);
    }

}
