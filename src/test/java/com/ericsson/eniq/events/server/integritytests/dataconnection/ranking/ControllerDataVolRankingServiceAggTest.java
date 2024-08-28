/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.dataconnection.ranking;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.dataconnection.ranking.ControllerDataVolumeRankingService;
import com.ericsson.eniq.events.server.test.queryresults.ResultTranslator;
import com.ericsson.eniq.events.server.test.queryresults.gsm.dataconnection.ControllerDataVolumeRankingResult;
import com.ericsson.eniq.events.server.test.schema.Nullable;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ethomit
 * @since 2011
 * 
 */

public class ControllerDataVolRankingServiceAggTest extends
        BaseDataIntegrityTest<ControllerDataVolumeRankingResult> {

    private ControllerDataVolumeRankingService ControllerDataVolumeRankingService;

    private static final String TEST_vendor_1 = "Testvendor1";

    private static final String TEST_vendor_2 = "Testvendor2";

    private static final String TEST_vendor_3 = "Testvendor3";

    private static final String TEST_HIERARCHY_3_1 = "testHierarchy1";

    private static final String TEST_HIERARCHY_3_2 = "testHierarchy2";

    private static final String TEST_HIERARCHY_3_3 = "testHierarchy3";

    private static final Long TEST_HIER3_ID_1 = 111111111L;

    private static final Long TEST_HIER3_ID_2 = 222222222L;

    private static final Long TEST_HIER3_ID_3 = 333333333L;

    private static final long MegaByte_2_Byte_Ratio = 1048576;

    @Before
    public void onSetUp() throws Exception {
        ControllerDataVolumeRankingService = new ControllerDataVolumeRankingService();
        attachDependencies(ControllerDataVolumeRankingService);
        createTables();
        insertData();
    }

    /*
     * The expected outcome is for vendor_3 to rank first, then vendor_2 and
     * finally vendor_1
     */
    @Test
    public void testGSMDataConnectionControllerByDataVolumeRankingOneWeekAggTest() throws Exception {

        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(DISPLAY_PARAM, GRID);
        requestParameters.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        requestParameters.putSingle(TZ_OFFSET, "+0100");
        requestParameters.putSingle(MAX_ROWS, "10");

        final String json = runQuery(ControllerDataVolumeRankingService, requestParameters);
        final ResultTranslator<ControllerDataVolumeRankingResult> resultTranslator = getTranslator();
        final List<ControllerDataVolumeRankingResult> rankingResult = resultTranslator.translateResult(json,
                ControllerDataVolumeRankingResult.class);

        assertThat("There should be exactly 3 results!", rankingResult.size(), is(3));

        final ControllerDataVolumeRankingResult highestTotalVolController = rankingResult.get(0);
        assertThat(highestTotalVolController.getRank(), is(1));
        assertThat(highestTotalVolController.getVendor(), is(TEST_vendor_3));
        assertThat(highestTotalVolController.getController(), is(TEST_HIERARCHY_3_3));
        assertThat("highestTotalVolController should have download data volume of 9.0!",
                highestTotalVolController.getDownLinkDataVol(), is(9.0));
        assertThat("highestTotalVolController should have upload data volume of 9.0!",
                highestTotalVolController.getUpLinkDataVol(), is(9.0));
        assertThat("highestTotalVolController should have total data volume of 18.0!",
                highestTotalVolController.getTotalDataVol(), is(18.0));
        assertThat(highestTotalVolController.getHIER3_ID(), is(TEST_HIER3_ID_3));

        final ControllerDataVolumeRankingResult secondHighestTotalVolController = rankingResult.get(1);
        assertThat(secondHighestTotalVolController.getRank(), is(2));
        assertThat(secondHighestTotalVolController.getVendor(), is(TEST_vendor_2));
        assertThat(secondHighestTotalVolController.getController(), is(TEST_HIERARCHY_3_2));
        assertThat("secondHighestTotalVolController should have download data volume of 10.0!",
                secondHighestTotalVolController.getDownLinkDataVol(), is(10.0));
        assertThat("secondHighestTotalVolController should have upload data volume of 7.0!",
                secondHighestTotalVolController.getUpLinkDataVol(), is(7.0));
        assertThat("secondHighestTotalVolController should have total data volume of 17.0!",
                secondHighestTotalVolController.getTotalDataVol(), is(17.0));
        assertThat(secondHighestTotalVolController.getHIER3_ID(), is(TEST_HIER3_ID_2));

        final ControllerDataVolumeRankingResult thirdHighestTotalVolController = rankingResult.get(2);
        assertThat(thirdHighestTotalVolController.getRank(), is(3));
        assertThat(thirdHighestTotalVolController.getVendor(), is(TEST_vendor_1));
        assertThat(thirdHighestTotalVolController.getController(), is(TEST_HIERARCHY_3_1));
        assertThat("thirdHighestTotalVolController should have download data volume of 5.0!",
                thirdHighestTotalVolController.getDownLinkDataVol(), is(5.0));
        assertThat("thirdHighestTotalVolController should have upload data volume of 10.0!",
                thirdHighestTotalVolController.getUpLinkDataVol(), is(10.0));
        assertThat("thirdHighestTotalVolController should have total data volume of 15.0!",
                thirdHighestTotalVolController.getTotalDataVol(), is(15.0));
        assertThat(thirdHighestTotalVolController.getHIER3_ID(), is(TEST_HIER3_ID_1));

    }

    private void createTables() throws Exception {

        final Map<String, Nullable> columnsForEventsTable = new HashMap<String, Nullable>();
        columnsForEventsTable.put(DOWNLOAD_DATA_VOLUME, Nullable.CAN_BE_NULL);
        columnsForEventsTable.put(UPLOAD_DATA_VOLUME, Nullable.CAN_BE_NULL);
        columnsForEventsTable.put(TOTAL_DATA_VOLUME, Nullable.CAN_BE_NULL);
        columnsForEventsTable.put(DATETIME_ID, Nullable.CAN_BE_NULL);
        columnsForEventsTable.put(TAC, Nullable.CANNOT_BE_NULL);
        columnsForEventsTable.put(HIER3_ID, Nullable.CAN_BE_NULL);
        createTemporaryTable(TEMP_EVENT_E_GSM_PS_ALL_HIER3_DAY, columnsForEventsTable);

        final Map<String, Nullable> topologyTableColumns = new HashMap<String, Nullable>();

        topologyTableColumns.put(HIERARCHY_3, Nullable.CAN_BE_NULL);
        topologyTableColumns.put(VENDOR_PARAM_UPPER_CASE, Nullable.CAN_BE_NULL);
        topologyTableColumns.put(HIER3_ID, Nullable.CAN_BE_NULL);
        createTemporaryTable(TEMP_DIM_E_SGEH_HIER321, topologyTableColumns);

    }

    private void insertData() throws Exception {

        final String dateTime = DateTimeUtilities.getDateMinus48Hours();

        insertRowToPSAGG(dateTime, SAMPLE_TAC, 5, 10, 15, TEST_HIER3_ID_1);
        insertRowToPSAGG(dateTime, SAMPLE_TAC, 10, 7, 17, TEST_HIER3_ID_2);
        insertRowToPSAGG(dateTime, SAMPLE_TAC, 9, 9, 18, TEST_HIER3_ID_3);

        insertRowIntoHier321Table(TEST_vendor_1, TEST_HIERARCHY_3_1, TEST_HIER3_ID_1);
        insertRowIntoHier321Table(TEST_vendor_2, TEST_HIERARCHY_3_2, TEST_HIER3_ID_2);
        insertRowIntoHier321Table(TEST_vendor_3, TEST_HIERARCHY_3_3, TEST_HIER3_ID_3);

    }

    private void insertRowToPSAGG(final String date, final int tac, final double downDataVol, final double upDataVol,
            final double totalDataVol, final Long hier3_id) throws SQLException {

        final Map<String, Object> valuesForTable = new HashMap<String, Object>();

        valuesForTable.put(DATETIME_ID, date);
        valuesForTable.put(TAC, tac);
        valuesForTable.put(DOWNLOAD_DATA_VOLUME, (downDataVol * MegaByte_2_Byte_Ratio));
        valuesForTable.put(UPLOAD_DATA_VOLUME, (upDataVol * MegaByte_2_Byte_Ratio));
        valuesForTable.put(TOTAL_DATA_VOLUME, (totalDataVol * MegaByte_2_Byte_Ratio));
        valuesForTable.put(HIER3_ID, hier3_id);

        insertRow(TEMP_EVENT_E_GSM_PS_ALL_HIER3_DAY, valuesForTable);

    }

    private void insertRowIntoHier321Table(final String vendor, final String controller, final Long hier3Id)
            throws SQLException {

        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(VENDOR_PARAM_UPPER_CASE, vendor);
        valuesForTable.put(HIERARCHY_3, controller);
        valuesForTable.put(HIER3_ID, hier3Id);

        insertRow(TEMP_DIM_E_SGEH_HIER321, valuesForTable);
    }

}
