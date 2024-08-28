/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.callfailure.ranking;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.DATETIME_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DISPLAY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.HIER3_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MAX_ROWS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TAC;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.VENDOR_PARAM_UPPER_CASE;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.BSC1;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.BSC2;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.BSC3;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.DEFAULT_MAX_ROWS;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.ERICSSON;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.EXCLUSIVE_TAC_GROUP;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.FIVE_MINUTES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GRID;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GROUP_NAME;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.HIERARCHY_3;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.RAT;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.RAT_FOR_GSM;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_TAC_2;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TEST_VALUE_GSM_CONTROLLER1_NAME;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TEST_VALUE_GSM_CONTROLLER2_NAME;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TEST_VALUE_GSM_CONTROLLER3_NAME;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TEST_VALUE_GSM_HIER3_ID_BSC1;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TEST_VALUE_GSM_HIER3_ID_BSC2;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TEST_VALUE_GSM_HIER3_ID_BSC3;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_DIM_E_SGEH_HIER321;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_GSM_CFA_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_GROUP_TYPE_E_TAC;
import static org.junit.Assert.assertTrue;

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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.ranking.ControllerRankingService;
import com.ericsson.eniq.events.server.test.queryresults.ResultTranslator;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.GSMCallFailureControllerRankingResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class GSMCallFailureControllerCallDropRankingServiceRawTest extends
        BaseDataIntegrityTest<GSMCallFailureControllerRankingResult> {

    private ControllerRankingService gsmCFAControllerRankingService;

    /**
     * 1. Create tables.
     * 2. Insert test data to the tables.
     * @throws Exception
     */
    @Before
    public void onSetUp() throws Exception {
        gsmCFAControllerRankingService = new ControllerRankingService();
        attachDependencies(gsmCFAControllerRankingService);
        createTables();

    }

    @Test
    public void testGetRankingData_Controller_GSMCFA() throws Exception {

        final List<GSMCallFailureControllerRankingResult> expectedResult = insertDataForSixEventsInThreeControllerAndGetExpectedResults();
        final List<GSMCallFailureControllerRankingResult> actualResult = getData();
        assertTrue(expectedResult.equals(actualResult));
    }

    @Test
    public void testGetRankingData_Controller_GSMCFA_WithTACExclusion() throws Exception {

        final List<GSMCallFailureControllerRankingResult> expectedResult = insertDataForSixEventsInThreeControllerAndGetExpectedResults();
        tweakExpectedResultForExclusiveTACs(expectedResult);
        insertDataIntoTacGroupTable();
        final List<GSMCallFailureControllerRankingResult> actualResult = getData();
        assertTrue(expectedResult.equals(actualResult));
    }

    private void tweakExpectedResultForExclusiveTACs(final List<GSMCallFailureControllerRankingResult> expectedResult) {
        //remove the events that contain TAC in the exclusive tac group
        expectedResult.remove(1);
        expectedResult.remove(0);
        //promote the rank
        expectedResult.get(0).setRank(1);
    }

    private List<GSMCallFailureControllerRankingResult> getData() throws Exception {

        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(DISPLAY_PARAM, GRID);
        requestParameters.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        requestParameters.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        requestParameters.putSingle(MAX_ROWS, DEFAULT_MAX_ROWS);

        final String json = runQuery(gsmCFAControllerRankingService, requestParameters);

        final ResultTranslator<GSMCallFailureControllerRankingResult> rt = getTranslator();
        final List<GSMCallFailureControllerRankingResult> actualResult = rt.translateResult(json,
                GSMCallFailureControllerRankingResult.class);
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
        columnsForRawTable.add(HIER3_ID);
        columnsForRawTable.add(DATETIME_ID);
        columnsForRawTable.add(TAC);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columnsForRawTable);

        final Collection<String> columnsForDIMTable = new ArrayList<String>();
        columnsForDIMTable.add(RAT);
        columnsForDIMTable.add(HIERARCHY_3);
        columnsForDIMTable.add(VENDOR_PARAM_UPPER_CASE);
        columnsForDIMTable.add(HIER3_ID);
        createTemporaryTable(TEMP_DIM_E_SGEH_HIER321, columnsForDIMTable);

    }

    private void populateHier321Table() throws SQLException {
        insertRowIntoHier321Table(RAT_FOR_GSM, TEST_VALUE_GSM_CONTROLLER1_NAME, ERICSSON, TEST_VALUE_GSM_HIER3_ID_BSC1);
        insertRowIntoHier321Table(RAT_FOR_GSM, TEST_VALUE_GSM_CONTROLLER2_NAME, ERICSSON, TEST_VALUE_GSM_HIER3_ID_BSC2);
        insertRowIntoHier321Table(RAT_FOR_GSM, TEST_VALUE_GSM_CONTROLLER3_NAME, ERICSSON, TEST_VALUE_GSM_HIER3_ID_BSC3);
    }

    private void insertRowIntoHier321Table(final int rat, final String controller, final String vendor,
            final long hier3Id) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(RAT, rat);
        valuesForTable.put(HIERARCHY_3, controller);
        valuesForTable.put(VENDOR_PARAM_UPPER_CASE, vendor);
        valuesForTable.put(HIER3_ID, hier3Id);

        insertRow(TEMP_DIM_E_SGEH_HIER321, valuesForTable);
    }

    /**
     * This function is used to insert test data to the prepared tables.

     * @throws Exception
     */
    private List<GSMCallFailureControllerRankingResult> insertDataForSixEventsInThreeControllerAndGetExpectedResults()
            throws Exception {

        populateHier321Table();
        final String dateTime = DateTimeUtilities.getDateTimeMinus2Minutes();

        final List<GSMCallFailureControllerRankingResult> expectedResultsList = new ArrayList<GSMCallFailureControllerRankingResult>();
        insertRowsInRawAndPutExpectedResultInList(expectedResultsList, 1, BSC1, TEST_VALUE_GSM_HIER3_ID_BSC1, dateTime,
                3, SAMPLE_TAC);
        insertRowsInRawAndPutExpectedResultInList(expectedResultsList, 2, BSC2, TEST_VALUE_GSM_HIER3_ID_BSC2, dateTime,
                2, SAMPLE_TAC);
        insertRowsInRawAndPutExpectedResultInList(expectedResultsList, 3, BSC3, TEST_VALUE_GSM_HIER3_ID_BSC3, dateTime,
                1, SAMPLE_TAC_2);

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
            final List<GSMCallFailureControllerRankingResult> expectedResultsList, final int expectedRank,
            final String expectedController, final long hier3Id, final String datetime, final int instances,
            final int tac) throws SQLException {
        for (int i = 0; i < instances; i++) {
            final Map<String, Object> valuesForTable = new HashMap<String, Object>();
            valuesForTable.put(HIER3_ID, hier3Id);
            valuesForTable.put(DATETIME_ID, datetime);
            valuesForTable.put(TAC, tac);
            insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, valuesForTable);
        }
        expectedResultsList.add(getExpectedResult(expectedRank, ERICSSON, expectedController, instances));
    }

    private GSMCallFailureControllerRankingResult getExpectedResult(final int rank, final String vendor,
            final String controller, final int numFailures) {
        final GSMCallFailureControllerRankingResult expectedResult = new GSMCallFailureControllerRankingResult();
        expectedResult.setRank(rank);
        expectedResult.setVendor(vendor);
        expectedResult.setController(controller);
        expectedResult.setNumFailures(numFailures);
        return expectedResult;
    }
}
