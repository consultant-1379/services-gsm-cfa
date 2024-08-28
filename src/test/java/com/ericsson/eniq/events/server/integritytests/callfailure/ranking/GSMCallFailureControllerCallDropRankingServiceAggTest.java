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
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.VENDOR_PARAM_UPPER_CASE;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.BSC1;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.BSC2;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.BSC3;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.DEFAULT_MAX_ROWS;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.ERICSSON;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GRID;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.HIERARCHY_3;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.NO_OF_ERRORS;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.ONE_WEEK;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.RAT;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.RAT_FOR_GSM;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TEST_VALUE_GSM_CONTROLLER1_NAME;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TEST_VALUE_GSM_CONTROLLER2_NAME;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TEST_VALUE_GSM_CONTROLLER3_NAME;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TEST_VALUE_GSM_HIER3_ID_BSC1;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TEST_VALUE_GSM_HIER3_ID_BSC2;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TEST_VALUE_GSM_HIER3_ID_BSC3;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_DIM_E_SGEH_HIER321;
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

public class GSMCallFailureControllerCallDropRankingServiceAggTest extends
        BaseDataIntegrityTest<GSMCallFailureControllerRankingResult> {

    private ControllerRankingService gsmCFAControllerRankingService;
    private static final String TEMP_EVENT_E_GSM_CFA_HIER3_ERR_DAY = "#EVENT_E_GSM_CFA_HIER3_ERR_DAY";

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

    private List<GSMCallFailureControllerRankingResult> getData() throws Exception {

        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(DISPLAY_PARAM, GRID);
        requestParameters.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        requestParameters.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        requestParameters.putSingle(MAX_ROWS, DEFAULT_MAX_ROWS);

        final String json = runQuery(gsmCFAControllerRankingService, requestParameters);

        final ResultTranslator<GSMCallFailureControllerRankingResult> rt = getTranslator();
        final List<GSMCallFailureControllerRankingResult> actualResult = rt.translateResult(json,
                GSMCallFailureControllerRankingResult.class);
        return actualResult;
    }

    /**
    * Create the prepare test tables for testing.
    * 
    * agg table: EVENT_E_GSM_CFA_HIER321_ERR_15MIN
    * @throws Exception
    */
    private void createTables() throws Exception {

        final Collection<String> columnsForEventTable = new ArrayList<String>();
        columnsForEventTable.add(HIER3_ID);
        columnsForEventTable.add(DATETIME_ID);
        columnsForEventTable.add(NO_OF_ERRORS);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_HIER3_ERR_DAY, columnsForEventTable);

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
        final String dateTime = DateTimeUtilities.getDateTimeMinus48Hours();

        final List<GSMCallFailureControllerRankingResult> expectedResultsList = new ArrayList<GSMCallFailureControllerRankingResult>();
        insertRowsInAggAndPutExpectedResultInList(expectedResultsList, 1, BSC1, TEST_VALUE_GSM_HIER3_ID_BSC1, dateTime,
                3);
        insertRowsInAggAndPutExpectedResultInList(expectedResultsList, 2, BSC2, TEST_VALUE_GSM_HIER3_ID_BSC2, dateTime,
                2);
        insertRowsInAggAndPutExpectedResultInList(expectedResultsList, 3, BSC3, TEST_VALUE_GSM_HIER3_ID_BSC3, dateTime,
                1);

        return expectedResultsList;
    }

    /**
     * This function is used to insert no. of event with IMSI to the table.
     * @param hier321Id         hash id.
     * @param datetime          The event time.
     * @param instances         The number of times to enter the row in the raw event table
     * @throws SQLException
     */
    private void insertRowsInAggAndPutExpectedResultInList(
            final List<GSMCallFailureControllerRankingResult> expectedResultsList, final int expectedRank,
            final String expectedController, final long hier3Id, final String datetime, final int instances)
            throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(HIER3_ID, hier3Id);
        valuesForTable.put(DATETIME_ID, datetime);
        valuesForTable.put(NO_OF_ERRORS, instances);
        insertRow(TEMP_EVENT_E_GSM_CFA_HIER3_ERR_DAY, valuesForTable);
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
