/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.callfailure.ranking;

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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.ranking.AccessAreaRankingService;
import com.ericsson.eniq.events.server.test.queryresults.ResultTranslator;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.GSMCallFailureAccessAreaRankingResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ejoegaf
 * @since 2011
 *
 */
public class GSMCallFailureAccessAreaCallDropRankingServiceRawTest extends
        BaseDataIntegrityTest<GSMCallFailureAccessAreaRankingResult> {

    private AccessAreaRankingService gsmCFAAccessAreaRankingService;

    /**
     * 1. Create tables.
     * 2. Insert test data to the tables.
     * @throws Exception
     */
    @Before
    public void onSetUp() throws Exception {
        gsmCFAAccessAreaRankingService = new AccessAreaRankingService();
        attachDependencies(gsmCFAAccessAreaRankingService);
        createTables();

    }

    @Test
    public void testGetRankingData_AccessArea_GSMCFA() throws Exception {

        final List<GSMCallFailureAccessAreaRankingResult> expectedResult = insertDataForSixEventsInThreeAccessAreaAndGetExpectedResults();
        final List<GSMCallFailureAccessAreaRankingResult> actualResult = getData();
        assertTrue(expectedResult.equals(actualResult));
    }

    @Test
    public void testGetRankingData_AccessArea_GSMCFA_WithTACExclusion() throws Exception {

        final List<GSMCallFailureAccessAreaRankingResult> expectedResult = insertDataForSixEventsInThreeAccessAreaAndGetExpectedResults();
        tweakExpectedResultForExclusiveTACs(expectedResult);
        insertDataIntoTacGroupTable();
        final List<GSMCallFailureAccessAreaRankingResult> actualResult = getData();
        assertTrue(expectedResult.equals(actualResult));
    }

    private void tweakExpectedResultForExclusiveTACs(final List<GSMCallFailureAccessAreaRankingResult> expectedResult) {
        //remove the events that contain TAC in the exclusive tac group
        expectedResult.remove(1);
        expectedResult.remove(0);
        //promote the rank
        expectedResult.get(0).setRank(1);
    }

    private List<GSMCallFailureAccessAreaRankingResult> getData() throws Exception {

        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(DISPLAY_PARAM, GRID);
        requestParameters.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        requestParameters.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        requestParameters.putSingle(MAX_ROWS, DEFAULT_MAX_ROWS);

        final String json = runQuery(gsmCFAAccessAreaRankingService, requestParameters);

        validateAgainstGridDefinition(json, "GSM_CFA_CELL_RANKING");
        final ResultTranslator<GSMCallFailureAccessAreaRankingResult> rt = getTranslator();
        final List<GSMCallFailureAccessAreaRankingResult> actualResult = rt.translateResult(json,
                GSMCallFailureAccessAreaRankingResult.class);
        return actualResult;
    }

    private void insertDataIntoTacGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SAMPLE_TAC);
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    /**
    * Create the prepare test tables for testing.
    * 
    * raw table: EVENT_E_GSM_CFA_ERR_RAW
    * @throws Exception
    */
    private void createTables() throws Exception {

        final Collection<String> columnsForRawTable = new ArrayList<String>();
        columnsForRawTable.add(HIER321_ID);
        columnsForRawTable.add(DATETIME_ID);
        columnsForRawTable.add(TAC);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columnsForRawTable);

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

    /**
     * This function is used to insert test data to the prepared tables.

     * @throws Exception
     */
    private List<GSMCallFailureAccessAreaRankingResult> insertDataForSixEventsInThreeAccessAreaAndGetExpectedResults()
            throws Exception {

        populateHier321Table();
        final String dateTime = DateTimeUtilities.getDateTimeMinus2Minutes();

        final List<GSMCallFailureAccessAreaRankingResult> expectedResultsList = new ArrayList<GSMCallFailureAccessAreaRankingResult>();
        insertRowsInRawAndPutExpectedResultInList(expectedResultsList, 1, BSC1, TEST_VALUE_GSM_CELL1_NAME,
                GSM_HASH_HIER321_ID_BSC1_CELL1, dateTime, 3, SAMPLE_TAC);
        insertRowsInRawAndPutExpectedResultInList(expectedResultsList, 2, BSC1, TEST_VALUE_GSM_CELL2_NAME,
                GSM_HASH_HIER321_ID_BSC1_CELL2, dateTime, 2, SAMPLE_TAC);
        insertRowsInRawAndPutExpectedResultInList(expectedResultsList, 3, BSC1, TEST_VALUE_GSM_CELL3_NAME,
                GSM_HASH_HIER321_ID_BSC1_CELL3, dateTime, 1, SAMPLE_TAC_2);

        return expectedResultsList;
    }

    /**
     * This function is used to insert no. of event with IMSI to the table.
     * @param hier321Id         hash id.
     * @param datetime          The event time.
     * @param instances         The number of times to enter the row in the raw event table
     * @throws SQLException
     */
    private void insertRowsInRawAndPutExpectedResultInList(
            final List<GSMCallFailureAccessAreaRankingResult> expectedResultsList, final int expectedRank,
            final String expectedController, final String expectedAccessArea, final long hier321Id,
            final String datetime, final int instances, final int tac) throws SQLException {
        for (int i = 0; i < instances; i++) {
            final Map<String, Object> valuesForTable = new HashMap<String, Object>();
            valuesForTable.put(HIER321_ID, hier321Id);
            valuesForTable.put(DATETIME_ID, datetime);
            valuesForTable.put(TAC, tac);
            insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, valuesForTable);
        }
        expectedResultsList.add(getExpectedResult(expectedRank, ERICSSON, hier321Id, expectedController,
                expectedAccessArea, instances));
    }

    private GSMCallFailureAccessAreaRankingResult getExpectedResult(final int rank, final String vendor,
            final long hashId, final String controller, final String accessArea, final int numFailures) {
        final GSMCallFailureAccessAreaRankingResult expectedResult = new GSMCallFailureAccessAreaRankingResult();
        expectedResult.setRank(rank);
        expectedResult.setVendor(vendor);
        expectedResult.setController(controller);
        expectedResult.setAccessArea(accessArea);
        expectedResult.setNumFailures(numFailures);
        expectedResult.setHashId(hashId);
        return expectedResult;
    }
}
