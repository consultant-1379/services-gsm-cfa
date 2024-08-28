/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.serviceprovider.callfailure.roaming;

import javax.annotation.Resource;
import javax.ws.rs.core.MultivaluedMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.resources.automation.dataproviders.MCCMNCCategoryIDDataProvider;
import com.ericsson.eniq.events.server.serviceprovider.Service;

/**
 * @author eprjaya
 *
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = { "classpath:gsm-cfa-service-context.xml" })
public class RoamingSummaryOperatorAnalysisTest extends ServiceBaseTest {

    @Resource(name = "roamingByOperatorAnalysisService")
    private Service operatorDrillRoamingAnalysisService;

    @Test
    @Parameters(source = MCCMNCCategoryIDDataProvider.class)
    public void testGetData(final MultivaluedMap<String, String> requestParameters) {
        runQuery(requestParameters, operatorDrillRoamingAnalysisService);
    }

}
