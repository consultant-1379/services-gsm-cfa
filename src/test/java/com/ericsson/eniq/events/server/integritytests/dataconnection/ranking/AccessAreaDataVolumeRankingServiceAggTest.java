/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.dataconnection.ranking;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.dataconnection.ranking.AccessAreaDataVolumeRankingService;
import com.ericsson.eniq.events.server.test.queryresults.ResultTranslator;
import com.ericsson.eniq.events.server.test.queryresults.gsm.dataconnection.AccessAreaDataVolumeRankingServiceResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author eramiye
 * @since Dec 2011
 */
public class AccessAreaDataVolumeRankingServiceAggTest extends
        BaseDataIntegrityTest<AccessAreaDataVolumeRankingServiceResult> {

    private final static long MB_2_B = 1024 * 1024;

    private AccessAreaDataVolumeRankingService gsmDataConnectionNetworkDataVolumeRankingService;

    @Before
    public void onSetUp() throws Exception {
        gsmDataConnectionNetworkDataVolumeRankingService = new AccessAreaDataVolumeRankingService();
        attachDependencies(gsmDataConnectionNetworkDataVolumeRankingService);
        createTables();
    }

    @Test
    public void testGetRankingData() throws Exception {
        final List<AccessAreaDataVolumeRankingServiceResult> expectedResult = insertDataForSixEventsInThreeAccessAreaAndGetExpectedResults();
        final List<AccessAreaDataVolumeRankingServiceResult> actualResult = getData();
        // System.out.println(expectedResult);
        // System.out.println(actualResult);
        assertTrue(expectedResult.equals(actualResult));
    }

    private List<AccessAreaDataVolumeRankingServiceResult> getData() throws Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(DISPLAY_PARAM, GRID);
        requestParameters.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        requestParameters.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        requestParameters.putSingle(MAX_ROWS, DEFAULT_MAX_ROWS);
        // System.out.println("requestParameters: " + requestParameters);
        final String json = runQuery(gsmDataConnectionNetworkDataVolumeRankingService, requestParameters);

        validateAgainstGridDefinition(json, "ACCESSAREA_DATAVOLUME_RANKING");
        final ResultTranslator<AccessAreaDataVolumeRankingServiceResult> rt = getTranslator();
        final List<AccessAreaDataVolumeRankingServiceResult> actualResult = rt.translateResult(json,
                AccessAreaDataVolumeRankingServiceResult.class);
        return actualResult;
    }

    /*
     * Create tables in temp DB
     */
    private void createTables() throws Exception {
        final Collection<String> columnsForEventTable = new ArrayList<String>();
        columnsForEventTable.add(HIER321_ID);
        columnsForEventTable.add(DOWNLOAD_DATA_VOLUME);
        columnsForEventTable.add(UPLOAD_DATA_VOLUME);
        columnsForEventTable.add(TOTAL_DATA_VOLUME);
        columnsForEventTable.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_PS_ALL_HIER321_DAY, columnsForEventTable);

        final Collection<String> columnsForDIMTable = new ArrayList<String>();
        columnsForDIMTable.add(RAT);
        columnsForDIMTable.add(HIERARCHY_1);
        columnsForDIMTable.add(HIERARCHY_3);
        columnsForDIMTable.add(VENDOR_PARAM_UPPER_CASE);
        columnsForDIMTable.add(HIER321_ID);
        columnsForDIMTable.add(HIER3_ID);
        createTemporaryTable(TEMP_DIM_E_SGEH_HIER321, columnsForDIMTable);

    }

    private void populateHier321Table() throws SQLException {
        insertRowIntoHier321Table(RAT_FOR_GSM, TEST_VALUE_GSM_CELL1_NAME, TEST_VALUE_GSM_CONTROLLER1_NAME, ERICSSON,
                TEST_VALUE_GSM_HIER3_ID, GSM_HASH_HIER321_ID_BSC1_CELL1);
        insertRowIntoHier321Table(RAT_FOR_GSM, TEST_VALUE_GSM_CELL2_NAME, TEST_VALUE_GSM_CONTROLLER1_NAME, ERICSSON,
                TEST_VALUE_GSM_HIER3_ID, GSM_HASH_HIER321_ID_BSC1_CELL2);
        insertRowIntoHier321Table(RAT_FOR_GSM, TEST_VALUE_GSM_CELL3_NAME, TEST_VALUE_GSM_CONTROLLER1_NAME, ERICSSON,
                TEST_VALUE_GSM_HIER3_ID, GSM_HASH_HIER321_ID_BSC1_CELL3);
    }

    private void insertRowIntoHier321Table(final int rat, final String cell, final String controller,
            final String vendor, final long hier3Id, final long hier321Id) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(RAT, rat);
        valuesForTable.put(HIERARCHY_1, cell);
        valuesForTable.put(HIERARCHY_3, controller);
        valuesForTable.put(VENDOR_PARAM_UPPER_CASE, vendor);
        valuesForTable.put(HIER3_ID, hier3Id);
        valuesForTable.put(HIER321_ID, hier321Id);

        insertRow(TEMP_DIM_E_SGEH_HIER321, valuesForTable);
    }

    private List<AccessAreaDataVolumeRankingServiceResult> insertDataForSixEventsInThreeAccessAreaAndGetExpectedResults()
            throws Exception {
        populateHier321Table();
        final String dateTime = DateTimeUtilities.getDateMinus48Hours();

        final List<AccessAreaDataVolumeRankingServiceResult> expectedResultsList = new ArrayList<AccessAreaDataVolumeRankingServiceResult>();

        // final long[][] dataVolumes = { { 1 * MB_2_B, 2 * MB_2_B, 3 * MB_2_B }, {
        // 2 * MB_2_B, 2 * MB_2_B, 4 * MB_2_B },
        // { 2 * MB_2_B, 3 * MB_2_B, 5 * MB_2_B } };

        final long[] dataVolumes1 = { 1 * MB_2_B, 2 * MB_2_B, 3 * MB_2_B, 3 * MB_2_B, 1 * MB_2_B, 2 * MB_2_B,
                3 * MB_2_B, 3 * MB_2_B, 5 * MB_2_B };
        final long[] dataVolumes2 = { 2 * MB_2_B, 1 * MB_2_B, 2 * MB_2_B, 1 * MB_2_B, 3 * MB_2_B, 3 * MB_2_B,
                1 * MB_2_B, 3 * MB_2_B, 5 * MB_2_B };
        final long[] dataVolumes3 = { 3 * MB_2_B, 3 * MB_2_B, 1 * MB_2_B, 2 * MB_2_B, 2 * MB_2_B, 1 * MB_2_B,
                2 * MB_2_B, 3 * MB_2_B, 5 * MB_2_B };

        insertRowsInAggrAndPutExpectedResultInList(expectedResultsList, 1, ERICSSON, BSC1, TEST_VALUE_GSM_CELL1_NAME,
                GSM_HASH_HIER321_ID_BSC1_CELL1, dateTime, dataVolumes1);
        insertRowsInAggrAndPutExpectedResultInList(expectedResultsList, 2, ERICSSON, BSC1, TEST_VALUE_GSM_CELL2_NAME,
                GSM_HASH_HIER321_ID_BSC1_CELL2, dateTime, dataVolumes2);
        insertRowsInAggrAndPutExpectedResultInList(expectedResultsList, 3, ERICSSON, BSC1, TEST_VALUE_GSM_CELL3_NAME,
                GSM_HASH_HIER321_ID_BSC1_CELL3, dateTime, dataVolumes3);
        return expectedResultsList;
    }

    private void insertRowsInAggrAndPutExpectedResultInList(
            final List<AccessAreaDataVolumeRankingServiceResult> expectedResultsList, final int expectedRank,
            final String expectedVendor, final String expectedController, final String expectedAccessArea,
            final long expectedHier321Id, final String datetime, final long[] dataVolumes) throws SQLException {
        // assertTrue(instances <= dataVolumes.length);
        // for (int i = 0; i < dataVolumes.length; ++i) {
        // assertEquals(3, dataVolumes[i].length);
        // }

        long expectedUplinkDataVolume = 0L;
        long expectedDownlinkDataVolume = 0L;
        long expectedTotalDataVolume = 0L;
        for (int i = 0; i < 3; ++i) {
            final Map<String, Object> valuesForTable = new HashMap<String, Object>();
            valuesForTable.put(HIER321_ID, expectedHier321Id);
            valuesForTable.put(DOWNLOAD_DATA_VOLUME, dataVolumes[0]);
            valuesForTable.put(UPLOAD_DATA_VOLUME, dataVolumes[1]);
            valuesForTable.put(TOTAL_DATA_VOLUME, dataVolumes[2]);
            expectedDownlinkDataVolume += dataVolumes[0];
            expectedUplinkDataVolume += dataVolumes[1];
            expectedTotalDataVolume += dataVolumes[2];
            valuesForTable.put(DATETIME_ID, datetime);
            insertRow(TEMP_EVENT_E_GSM_PS_ALL_HIER321_DAY, valuesForTable);
        }
        expectedResultsList.add(new AccessAreaDataVolumeRankingServiceResult(expectedRank, expectedVendor,
                expectedController, expectedAccessArea, expectedDownlinkDataVolume / (double) MB_2_B,
                expectedUplinkDataVolume / (double) MB_2_B, expectedTotalDataVolume / (double) MB_2_B,
                expectedHier321Id));
    }

}
