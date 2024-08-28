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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.dataconnection.ranking.AccessAreaDataVolumeRankingService;
import com.ericsson.eniq.events.server.dataproviders.dataconnection.RankingDataProvider;

/**
 * @author eramiye
 * @since Dec 2011
 */
@RunWith(JUnitParamsRunner.class)
@ContextConfiguration(locations = {"classpath:gsm-cfa-service-context.xml"})
public class CellDataVolumeRankingIntegrationTest extends ServiceBaseTest {

  @Resource(name = "gsmDataConnectionAccessAreaDataVolumeRankingService")
  private AccessAreaDataVolumeRankingService gsmDataConnectionAccessAreaDataVolumeRankingService;

  @Test
  @Parameters(source = RankingDataProvider.class)
  public void testGetData(final MultivaluedMap<String, String> requestParameters) {
    runQuery(requestParameters, gsmDataConnectionAccessAreaDataVolumeRankingService);
  }
}
