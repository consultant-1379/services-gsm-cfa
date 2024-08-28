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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.ranking.CauseCodeRankingService;
import com.ericsson.eniq.events.server.test.queryresults.ResultTranslator;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.GSMCallFailureCauseCodeCallDropRankingResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ejoegaf
 * @since 2011
 *
 */
public class GSMCallFailureCauseCodeCallDropRankingServiceRawTest extends
        BaseDataIntegrityTest<GSMCallFailureCauseCodeCallDropRankingResult> {

    private CauseCodeRankingService gsmCauseCodeCFACallDropRankingService;

    /**
     * 1. Create tables.
     * 2. Insert test data to the tables.
     * @throws Exception
     */
    @Before
    public void onSetUp() throws Exception {
        gsmCauseCodeCFACallDropRankingService = new CauseCodeRankingService();
        attachDependencies(gsmCauseCodeCFACallDropRankingService);
        createTables();

    }

    @Test
    public void testGetRankingData_CauseCode_GSMCFA() throws Exception {
        final List<GSMCallFailureCauseCodeCallDropRankingResult> expectedResult = insertDataForSixEventsWithThreeCauseCodesAndGetExpectedResults();
        insertDataThatShouldNotAffectResults();
        final List<GSMCallFailureCauseCodeCallDropRankingResult> actualResult = getData();
        assertTrue(expectedResult.equals(actualResult));
    }

    @Test
    public void testGetRankingData_CauseCode_GSMCFA_WithTACExclusion() throws Exception {
        final List<GSMCallFailureCauseCodeCallDropRankingResult> expectedResult = insertDataForSixEventsWithThreeCauseCodesAndGetExpectedResults();
        insertDataThatShouldNotAffectResults();
        tweakExpectedResultForExclusiveTACs(expectedResult);
        insertDataIntoTacGroupTable();
        final List<GSMCallFailureCauseCodeCallDropRankingResult> actualResult = getData();
        assertTrue(expectedResult.equals(actualResult));
    }

    private void tweakExpectedResultForExclusiveTACs(
            final List<GSMCallFailureCauseCodeCallDropRankingResult> expectedResult) {
        //remove the events that contain TAC in the exclusive tac group
        expectedResult.remove(1);
        expectedResult.remove(0);
        //promote the rank
        expectedResult.get(0).setRank(1);
    }

    private List<GSMCallFailureCauseCodeCallDropRankingResult> getData() throws Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(DISPLAY_PARAM, GRID);
        requestParameters.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        requestParameters.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        requestParameters.putSingle(MAX_ROWS, DEFAULT_MAX_ROWS);

        final String json = runQuery(gsmCauseCodeCFACallDropRankingService, requestParameters);

        validateAgainstGridDefinition(json, "GSM_CFA_CC_CALL_DROPS_RANKING");
        final ResultTranslator<GSMCallFailureCauseCodeCallDropRankingResult> rt = getTranslator();
        final List<GSMCallFailureCauseCodeCallDropRankingResult> actualResult = rt.translateResult(json,
                GSMCallFailureCauseCodeCallDropRankingResult.class);
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
        columnsForRawTable.add(GSM_COLUMN_NAME_URGENCY_CONDITION);
        columnsForRawTable.add(TAC);
        columnsForRawTable.add(DATETIME_ID);
        columnsForRawTable.add(CATEGORY_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columnsForRawTable);

        final Collection<String> columnsForDIMTable = new ArrayList<String>();
        columnsForDIMTable.add(GSM_COLUMN_NAME_URGENCY_CONDITION);
        columnsForDIMTable.add(GSM_COLUMN_NAME_URGENCY_CONDITION_DESC);
        createTemporaryTable(TEMP_DIM_E_GSM_CFA_URGENCY_CONDITION, columnsForDIMTable);
    }

    private void populateDIMUrgencyConditionTable() throws SQLException {
        insertRowIntoDIMUrgencyConditionTable(GSM_URGENCY_CONDITION1_ID, GSM_URGENCY_CONDITION1_DESC);
        insertRowIntoDIMUrgencyConditionTable(GSM_URGENCY_CONDITION2_ID, GSM_URGENCY_CONDITION2_DESC);
        insertRowIntoDIMUrgencyConditionTable(GSM_URGENCY_CONDITION3_ID, GSM_URGENCY_CONDITION3_DESC);
    }

    private void insertRowIntoDIMUrgencyConditionTable(final String urgencyCondition,
            final String urgencyConditionDescription) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(GSM_COLUMN_NAME_URGENCY_CONDITION, urgencyCondition);
        valuesForTable.put(GSM_COLUMN_NAME_URGENCY_CONDITION_DESC, urgencyConditionDescription);
        insertRow(TEMP_DIM_E_GSM_CFA_URGENCY_CONDITION, valuesForTable);
    }

    /**
     * This function is used to insert test data to the prepared tables.

     * @throws Exception
     */
    private List<GSMCallFailureCauseCodeCallDropRankingResult> insertDataForSixEventsWithThreeCauseCodesAndGetExpectedResults()
            throws Exception {
        populateDIMUrgencyConditionTable();
        final String dateTime = DateTimeUtilities.getDateTimeMinus2Minutes();

        final List<GSMCallFailureCauseCodeCallDropRankingResult> expectedResultsList = new ArrayList<GSMCallFailureCauseCodeCallDropRankingResult>();
        insertRowsInRawAndPutExpectedResultInList(expectedResultsList, 1, GSM_URGENCY_CONDITION1_ID,
                GSM_URGENCY_CONDITION1_DESC, dateTime, 3, SAMPLE_TAC, GSM_CALL_DROP_CATEGORY_ID);
        insertRowsInRawAndPutExpectedResultInList(expectedResultsList, 2, GSM_URGENCY_CONDITION2_ID,
                GSM_URGENCY_CONDITION2_DESC, dateTime, 2, SAMPLE_TAC, GSM_CALL_DROP_CATEGORY_ID);
        insertRowsInRawAndPutExpectedResultInList(expectedResultsList, 3, GSM_URGENCY_CONDITION3_ID,
                GSM_URGENCY_CONDITION3_DESC, dateTime, 1, SAMPLE_TAC_2, GSM_CALL_DROP_CATEGORY_ID);

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
            final List<GSMCallFailureCauseCodeCallDropRankingResult> expectedResultsList, final int expectedRank,
            final String causeCodeId, final String causeCodeDescription, final String datetime, final int instances,
            final int tac, final String catId) throws SQLException {
        for (int i = 0; i < instances; i++) {
            insertRowInRawTable(causeCodeId, datetime, tac, catId);
        }
        expectedResultsList.add(getExpectedResult(expectedRank, causeCodeDescription, causeCodeId, instances));
    }

    private void insertRowInRawTable(final String causeCodeId, final String datetime, final int tac, final String catId)
            throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(GSM_COLUMN_NAME_URGENCY_CONDITION, causeCodeId);
        valuesForTable.put(CATEGORY_ID, catId);
        valuesForTable.put(TAC, tac);
        valuesForTable.put(DATETIME_ID, datetime);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, valuesForTable);
    }

    private GSMCallFailureCauseCodeCallDropRankingResult getExpectedResult(final int rank,
            final String causeCodeDescription, final String causeCodeId, final int numFailures) {
        final GSMCallFailureCauseCodeCallDropRankingResult expectedResult = new GSMCallFailureCauseCodeCallDropRankingResult();
        expectedResult.setRank(rank);
        expectedResult.setCauseCodeDescription(causeCodeDescription);
        expectedResult.setCauseCodeId(causeCodeId);
        expectedResult.setNumFailures(numFailures);
        return expectedResult;
    }

    private void insertDataThatShouldNotAffectResults() throws SQLException {
        final String oldDateTime = DateTimeUtilities.getDateTimeMinus25Minutes();
        final String relevantDateTime = DateTimeUtilities.getDateTimeMinus2Minutes();
        //not in timerange of query
        insertRowInRawTable(GSM_URGENCY_CONDITION1_ID, oldDateTime, 1, GSM_CALL_DROP_CATEGORY_ID);
        //event id is not a call drop
        insertRowInRawTable(GSM_URGENCY_CONDITION1_ID, relevantDateTime, 1, GSM_CALL_SETUP_FAILURE_CATEGORY_ID);
    }
}
