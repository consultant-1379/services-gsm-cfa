package com.ericsson.eniq.events.server.serviceprovider.callfailure.cc;

import javax.annotation.Resource;
import javax.ws.rs.core.MultivaluedMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import com.ericsson.eniq.events.server.dataproviders.ControllerGroupBreakdownServiceDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.cc.ControllerGroupBreakdownService;

/**
 * @author elasabu
 * @since 2012
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class ControllerGroupBreakdownServiceIntegrationTest extends ServiceBaseTest {

    @Resource(name = "gsmControllerGroupBreakdownService")
    private ControllerGroupBreakdownService gsmControllerGroupBreakdownService;

    @Test
    @Parameters(source = ControllerGroupBreakdownServiceDataProvider.class)
    public void testGetDataForBscGroup(final MultivaluedMap<String, String> requestParameters) {
        final String result = runQuery(requestParameters, gsmControllerGroupBreakdownService);
        jsonAssertUtils.assertJSONSucceeds(result);
    }

    @Test
    @Parameters(source = ControllerGroupBreakdownServiceDataProvider.class)
    public void testGetDataBscGroupAsCSV(final MultivaluedMap<String, String> requestParameters) {
        runQueryForCSV(requestParameters, gsmControllerGroupBreakdownService);
    }

}
