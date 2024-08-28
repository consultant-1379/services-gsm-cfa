/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.callfailure.cc;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.junit.Assert.*;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.integritytests.stubs.ReplaceTablesWithTempTablesTemplateUtils;
import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.cc.ControllerCCService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.GSMCallFailureControllerCauseCodeAnalysisResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ejoegaf
 * @since 2011
 *
 */

public class GSMCallFailureControllerCauseCodeAnalysisAggTest extends
        BaseDataIntegrityTest<GSMCallFailureControllerCauseCodeAnalysisResult> {

    private static final String TEST_VALUE_HIER3_ID_BSC1 = "5386564559998864911";

    private static final String TEST_VALUE_HIER3_ID_BSC2 = "4027908921882107646";

    private static final String TEST_VALUE_CAUSE_GROUP_1 = "1";

    private static final String TEST_VALUE_CAUSE_GROUP_1_DESC = "EXCESSIVE TA";

    private static final String TEST_VALUE_CAUSE_GROUP_2 = "2";

    private static final String TEST_VALUE_CAUSE_GROUP_2_DESC = "SUDDENLY LOST CONNECTION";

    private static final String TEST_VALUE_CAUSE_GROUP_3 = "3";

    private static final String TEST_VALUE_CAUSE_GROUP_3_DESC = "LOW SS BOTHLINK";

    private ControllerCCService service;

    @Before
    public void setup() throws Exception {
        service = new ControllerCCService();

        attachDependencies(service);
        createEventTables();
        createLookupTables();
    }

    @Test
    public void testOneDayQuery() throws URISyntaxException, Exception {
        insertAllLookupData();
        final List<GSMCallFailureControllerCauseCodeAnalysisResult> expectedResult = insertEventDataAndGetExpectedResult(TEST_VALUE_HIER3_ID_BSC1);
        final String actualJSONResultStr = setParamsAndRunQuery(ONE_DAY);
        verifyResult(actualJSONResultStr, expectedResult);
    }

    @Test
    public void testOneDayQueryWithIrrelevantData() throws URISyntaxException, Exception {
        insertAllLookupData();
        final List<GSMCallFailureControllerCauseCodeAnalysisResult> expectedResult = insertEventDataAndGetExpectedResult(TEST_VALUE_HIER3_ID_BSC1);
        insertIrrelevantDataForOneDayQuery();
        final String actualJSONResultStr = setParamsAndRunQuery(ONE_DAY);
        verifyResult(actualJSONResultStr, expectedResult);
    }

    @Test
    public void testOneDayQueryWithExclusiveTAC() throws URISyntaxException, Exception {
        insertAllLookupData();
        final List<GSMCallFailureControllerCauseCodeAnalysisResult> expectedResult = insertEventDataAndGetExpectedResult(TEST_VALUE_HIER3_ID_BSC1);
        tweakExpectedResultForExclusiveTACs(expectedResult);
        insertTacIntoExclusiveTacGroup(SAMPLE_TAC_2);
        final String actualJSONResultStr = setParamsAndRunQuery(ONE_DAY);
        verifyResult(actualJSONResultStr, expectedResult);
    }

    private String setParamsAndRunQuery(final String mins) {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TIME_QUERY_PARAM, mins);
        requestParameters.add(NODE_PARAM, TEST_BSC1_NODE);
        requestParameters.add(TYPE_PARAM, BSC);
        requestParameters.add(DISPLAY_PARAM, GRID_PARAM);
        //FAILURE_TYPE_PARAM value doesnt matter since it is a Extended Cause Group flow
        requestParameters.add(FAILURE_TYPE_PARAM, GSM_CALL_SETUP_FAILURES_CATEGORY_ID_DESC);
        return runQuery(service, requestParameters);
    }

    private void verifyResult(final String json,
            final List<GSMCallFailureControllerCauseCodeAnalysisResult> expectedResults) throws Exception {
        assertJSONSucceeds(json);
        validateAgainstGridDefinition(json, "GSM_NETWORK_CAUSE_CODE_ANALYSIS_BSC");
        final List<GSMCallFailureControllerCauseCodeAnalysisResult> actualResults = getTranslator().translateResult(
                json, GSMCallFailureControllerCauseCodeAnalysisResult.class);

        assertEquals(expectedResults, actualResults);
    }

    private void tweakExpectedResultForExclusiveTACs(
            final List<GSMCallFailureControllerCauseCodeAnalysisResult> expectedResult) {
        //TAC in the exclusive tac group means that although the event is in the agg data, it should not be used in 
        //  calculations for impacted subscribers
        expectedResult.get(2).setNumImpactedSubs(0);
    }

    private void insertIrrelevantDataForOneDayQuery() throws SQLException {
        //event with irrelevant time
        final String dateTimeNowMinus46Hours = DateTimeUtilities.getDateTimeMinusHours(46);
        insertRowToRaw(TEST_VALUE_HIER3_ID_BSC1, TEST_VALUE_CAUSE_GROUP_1, SAMPLE_TAC, dateTimeNowMinus46Hours);
        insertRowToAgg(TEST_VALUE_HIER3_ID_BSC1, TEST_VALUE_CAUSE_GROUP_1, 1, dateTimeNowMinus46Hours);

        //event with irrelevant controller 
        final String dateTimeNowMinus27Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(27 + GSM_CFA_LATENCY_ON_ONE_DAY_QUERY);
        insertRowToRaw(TEST_VALUE_HIER3_ID_BSC2, TEST_VALUE_CAUSE_GROUP_1, SAMPLE_TAC, dateTimeNowMinus27Mins);
        insertRowToAgg(TEST_VALUE_HIER3_ID_BSC2, TEST_VALUE_CAUSE_GROUP_1, 1, dateTimeNowMinus27Mins);

        //unused TAC in EXCLUSIVE_TAC group
        insertTacIntoExclusiveTacGroup(SAMPLE_EXCLUSIVE_TAC);
    }

    private List<GSMCallFailureControllerCauseCodeAnalysisResult> insertEventDataAndGetExpectedResult(
            final String expectedController) throws Exception {
        final String dateTimeNowMinus27Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(27 + GSM_CFA_LATENCY_ON_ONE_DAY_QUERY);
        final String dateTimeNowMinus26Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(26 + GSM_CFA_LATENCY_ON_ONE_DAY_QUERY);
        final String dateTimeNowMinus25Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(25 + GSM_CFA_LATENCY_ON_ONE_DAY_QUERY);

        final List<GSMCallFailureControllerCauseCodeAnalysisResult> expectedResult = new ArrayList<GSMCallFailureControllerCauseCodeAnalysisResult>();
        expectedResult.add(insertDataToDBAndGetExpectedResult(expectedController, TEST_VALUE_CAUSE_GROUP_1_DESC,
                TEST_VALUE_CAUSE_GROUP_1, SAMPLE_TAC, dateTimeNowMinus27Mins, 3));
        expectedResult.add(insertDataToDBAndGetExpectedResult(expectedController, TEST_VALUE_CAUSE_GROUP_2_DESC,
                TEST_VALUE_CAUSE_GROUP_2, SAMPLE_TAC, dateTimeNowMinus26Mins, 2));
        expectedResult.add(insertDataToDBAndGetExpectedResult(expectedController, TEST_VALUE_CAUSE_GROUP_3_DESC,
                TEST_VALUE_CAUSE_GROUP_3, SAMPLE_TAC_2, dateTimeNowMinus25Mins, 1));

        return expectedResult;
    }

    private GSMCallFailureControllerCauseCodeAnalysisResult insertDataToDBAndGetExpectedResult(
            final String controllerHashId, final String causeCodeDesc, final String causeCodeId, final int tac,
            final String time, final int noInstances) throws SQLException {
        for (int i = 0; i < noInstances; i++) {
            insertRowToRaw(controllerHashId, causeCodeId, tac, time);
        }
        insertRowToAgg(controllerHashId, causeCodeId, noInstances, time);
        return getExpectedResult(causeCodeDesc, causeCodeId, noInstances, 1);
    }

    private GSMCallFailureControllerCauseCodeAnalysisResult getExpectedResult(final String causeCodeDesc,
            final String causeCodeId, final int numFailures, final int numImpactedSubs) {
        final GSMCallFailureControllerCauseCodeAnalysisResult expectedResult = new GSMCallFailureControllerCauseCodeAnalysisResult();
        expectedResult.setCauseCodeDesc(causeCodeDesc);
        expectedResult.setCauseCodeId(causeCodeId);
        expectedResult.setNumFailures(numFailures);
        expectedResult.setNumImpactedSubs(numImpactedSubs);
        expectedResult.setNode(TEST_BSC1_NODE);
        return expectedResult;
    }

    private void insertRowToRaw(final String controllerHashId, final String urgencyCondition, final int tac,
            final String time) throws SQLException {
        final Map<String, Object> dataForEventTable = new HashMap<String, Object>();
        dataForEventTable.put(HIER3_ID, controllerHashId);
        dataForEventTable.put(TAC, tac);
        dataForEventTable.put(CAUSE_GROUP, urgencyCondition);
        dataForEventTable.put(DATETIME_ID, time);
        dataForEventTable.put(IMSI, TEST_VALUE_IMSI);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataForEventTable);
    }

    private void insertRowToAgg(final String controllerHashId, final String urgencyCondition, final int noFailures,
            final String time) throws SQLException {
        final Map<String, Object> dataForEventTable = new HashMap<String, Object>();
        dataForEventTable.put(HIER3_ID, controllerHashId);
        dataForEventTable.put(NO_OF_ERRORS, noFailures);
        dataForEventTable.put(CAUSE_GROUP, urgencyCondition);
        dataForEventTable.put(DATETIME_ID, time);
        insertRow(TEMP_EVENT_E_GSM_CFA_HIER3_CG_ERR_15MIN, dataForEventTable);
    }

    private void createLookupTables() throws Exception {
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_CAUSE_GROUP, "DIM_E_GSM_CFA_CAUSE_GROUP", CAUSE_GROUP,
                CAUSE_GROUP_DESC);
    }

    void createAndReplaceLookupTable(final String tempTableName, final String tableNameToReplace,
            final String... columns) throws Exception {
        final Collection<String> columnsForTable = new ArrayList<String>();
        for (final String column : columns) {
            columnsForTable.add(column);
        }
        createTemporaryTable(tempTableName, columnsForTable);
        ReplaceTablesWithTempTablesTemplateUtils.addTableNameToReplace(tableNameToReplace, tempTableName);
    }

    private void insertAllLookupData() throws SQLException {
        insertUrgencyConditionLookupData();
    }

    private void insertRowToUrgencyConditionTable(final String urgencyCondition,
            final String urgencyConditionDescription) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(CAUSE_GROUP, urgencyCondition);
        valuesForTable.put(CAUSE_GROUP_DESC, urgencyConditionDescription);
        insertRow(TEMP_DIM_E_GSM_CFA_CAUSE_GROUP, valuesForTable);
    }

    private void insertUrgencyConditionLookupData() throws SQLException {
        insertRowToUrgencyConditionTable(TEST_VALUE_CAUSE_GROUP_1, TEST_VALUE_CAUSE_GROUP_1_DESC);
        insertRowToUrgencyConditionTable(TEST_VALUE_CAUSE_GROUP_2, TEST_VALUE_CAUSE_GROUP_2_DESC);
        insertRowToUrgencyConditionTable(TEST_VALUE_CAUSE_GROUP_3, TEST_VALUE_CAUSE_GROUP_3_DESC);
    }

    private void insertTacIntoExclusiveTacGroup(final int tac) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, tac);
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    private void createEventTables() throws Exception {
        final Collection<String> columnsForRawTable = new ArrayList<String>();
        columnsForRawTable.add(HIER3_ID);
        columnsForRawTable.add(TAC);
        columnsForRawTable.add(CAUSE_GROUP);
        columnsForRawTable.add(DATETIME_ID);
        columnsForRawTable.add(IMSI);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columnsForRawTable);

        final Collection<String> columnsForAggTable = new ArrayList<String>();
        columnsForAggTable.add(HIER3_ID);
        columnsForAggTable.add(CAUSE_GROUP);
        columnsForAggTable.add(DATETIME_ID);
        columnsForAggTable.add(NO_OF_ERRORS);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_HIER3_CG_ERR_15MIN, columnsForAggTable);
    }

}
