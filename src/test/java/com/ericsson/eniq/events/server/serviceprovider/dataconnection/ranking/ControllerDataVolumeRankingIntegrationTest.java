/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.serviceprovider.dataconnection.ranking;

import com.ericsson.eniq.events.server.dataproviders.dataconnection.RankingDataProvider;
import com.ericsson.eniq.events.server.resources.automation.ServiceBaseTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.dataconnection.ranking.ControllerDataVolumeRankingService;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import javax.annotation.Resource;
import javax.ws.rs.core.MultivaluedMap;

/**
 * @author ethomit
 * 
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = {"classpath:gsm-cfa-service-context.xml"})
public class ControllerDataVolumeRankingIntegrationTest extends ServiceBaseTest {

  @Resource(name = "gsmDataConnectionControllerDataVolumeRankingService")
  private ControllerDataVolumeRankingService gsmDataConnectionControllerDataVolumeRankingService;

  @Test
  @Parameters(source = RankingDataProvider.class)
  public void testGetData(final MultivaluedMap<String, String> requestParameters) {
    runQuery(requestParameters, gsmDataConnectionControllerDataVolumeRankingService);
  }

}
