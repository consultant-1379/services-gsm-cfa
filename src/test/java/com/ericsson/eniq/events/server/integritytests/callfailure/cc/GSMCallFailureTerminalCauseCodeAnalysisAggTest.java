package com.ericsson.eniq.events.server.integritytests.callfailure.cc;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.resources.gsm.GSMApplicationConstants.*;
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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.cc.TerminalCCService;
import com.ericsson.eniq.events.server.test.common.ApplicationTestConstants;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.GSMCallFailureTerminalCauseCodeAnalysisResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class GSMCallFailureTerminalCauseCodeAnalysisAggTest extends
        BaseDataIntegrityTest<GSMCallFailureTerminalCauseCodeAnalysisResult> {

    private static final String TEST_VALUE_CAUSE_GROUP_1 = "1";

    private static final String TEST_VALUE_CAUSE_GROUP_1_DESC = "EXCESSIVE TA";

    private static final String TEST_VALUE_CAUSE_GROUP_2 = "2";

    private static final String TEST_VALUE_CAUSE_GROUP_2_DESC = "SUDDENLY LOST CONNECTION";

    private static final String TEST_VALUE_CAUSE_GROUP_3 = "3";

    private static final String TEST_VALUE_CAUSE_GROUP_3_DESC = "LOW SS BOTHLINK";

    private TerminalCCService service;

    @Before
    public void setup() throws Exception {
        service = new TerminalCCService();

        attachDependencies(service);
        createEventTables();
        createLookupTables();
    }

    @Test
    public void testOneDayQueryCallDrop() throws URISyntaxException, Exception {
        insertAllLookupData();
        final List<GSMCallFailureTerminalCauseCodeAnalysisResult> expectedResult = insertEventDataAndGetExpectedResult(SAMPLE_TAC,
        		GSM_CALL_DROP_CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID);
        final String actualJSONResultStr = setParamsAndRunQuery(ONE_DAY, GSM_CALL_DROP_CATEGORY_ID);
        verifyResult(actualJSONResultStr, expectedResult);
    }

    @Test
    public void testOneDayQueryWithIrrelevantDataCallDrop() throws URISyntaxException, Exception {
        insertAllLookupData();
        final List<GSMCallFailureTerminalCauseCodeAnalysisResult> expectedResult = insertEventDataAndGetExpectedResult(SAMPLE_TAC, 
        		GSM_CALL_DROP_CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID);
        insertIrrelevantDataForOneDayQuery(GSM_CALL_DROP_CATEGORY_ID);
        final String actualJSONResultStr = setParamsAndRunQuery(ONE_DAY, GSM_CALL_DROP_CATEGORY_ID);
        verifyResult(actualJSONResultStr, expectedResult);
    }

    @Test
    public void testOneDayQueryWithExclusiveTACCallDrop() throws URISyntaxException, Exception {
        insertAllLookupData();
        final List<GSMCallFailureTerminalCauseCodeAnalysisResult> expectedResult = insertEventDataAndGetExpectedResult(SAMPLE_TAC,
        		GSM_CALL_DROP_CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID);
        insertExclusiveTacDataForThirtyMinsQuery(GSM_CALL_DROP_CATEGORY_ID);
        insertTacIntoExclusiveTacGroup(SAMPLE_TAC_2);
        final String actualJSONResultStr = setParamsAndRunQuery(ONE_DAY, GSM_CALL_DROP_CATEGORY_ID);
        verifyResult(actualJSONResultStr, expectedResult);
    }
    
    @Test
    public void testOneDayQueryCallSetupFailure() throws URISyntaxException, Exception {
        insertAllLookupData();
        final List<GSMCallFailureTerminalCauseCodeAnalysisResult> expectedResult = insertEventDataAndGetExpectedResult(SAMPLE_TAC,
        		GSM_CALL_SETUP_FAILURE_CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID);
        final String actualJSONResultStr = setParamsAndRunQuery(ONE_DAY, GSM_CALL_SETUP_FAILURE_CATEGORY_ID);
        verifyResult(actualJSONResultStr, expectedResult);
    }

    @Test
    public void testOneDayQueryWithIrrelevantDataCallSetupFailure() throws URISyntaxException, Exception {
        insertAllLookupData();
        final List<GSMCallFailureTerminalCauseCodeAnalysisResult> expectedResult = insertEventDataAndGetExpectedResult(SAMPLE_TAC, 
        		GSM_CALL_SETUP_FAILURE_CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID);
        insertIrrelevantDataForOneDayQuery(GSM_CALL_SETUP_FAILURE_CATEGORY_ID);
        final String actualJSONResultStr = setParamsAndRunQuery(ONE_DAY, GSM_CALL_SETUP_FAILURE_CATEGORY_ID);
        verifyResult(actualJSONResultStr, expectedResult);
    }

    @Test
    public void testOneDayQueryWithExclusiveTACCallSetupFailure() throws URISyntaxException, Exception {
        insertAllLookupData();
        final List<GSMCallFailureTerminalCauseCodeAnalysisResult> expectedResult = insertEventDataAndGetExpectedResult(SAMPLE_TAC,
        		GSM_CALL_SETUP_FAILURE_CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID);
        insertExclusiveTacDataForThirtyMinsQuery(GSM_CALL_SETUP_FAILURE_CATEGORY_ID);
        insertTacIntoExclusiveTacGroup(SAMPLE_TAC_2);
        final String actualJSONResultStr = setParamsAndRunQuery(ONE_DAY, GSM_CALL_SETUP_FAILURE_CATEGORY_ID);
        verifyResult(actualJSONResultStr, expectedResult);
    }

    private void insertExclusiveTacDataForThirtyMinsQuery(final String categoryId) throws SQLException {
        final String dateTimeNowMinus27Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(27 + GSM_CFA_LATENCY_ON_THIRTY_MIN_QUERY);
        insertRowToRaw(TEST_VALUE_CAUSE_GROUP_1, SAMPLE_TAC_2, dateTimeNowMinus27Mins, categoryId);
        insertRowToAgg(SAMPLE_TAC_2, TEST_VALUE_CAUSE_GROUP_1, 1, dateTimeNowMinus27Mins, categoryId);

    }

    private String setParamsAndRunQuery(final String mins, final String categoryId) {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TIME_QUERY_PARAM, mins);
        requestParameters.add(TYPE_MAN, MANUFACTURER_FOR_SAMPLE_TAC_2);
        requestParameters.add(MODEL, MODEL_NO);
        requestParameters.add(TAC_PARAM, "101800");
        requestParameters.add(TYPE_PARAM, TAC);
        requestParameters.add(CATEGORY_ID, categoryId);
        requestParameters.add(FAILURE_TYPE_PARAM, CALL_DROP);
        return runQuery(service, requestParameters);
    }

    private void verifyResult(final String json,
            final List<GSMCallFailureTerminalCauseCodeAnalysisResult> expectedResults) throws Exception {
        assertJSONSucceeds(json);
        validateAgainstGridDefinition(json, "GSM_NETWORK_CAUSE_CODE_ANALYSIS_BSC");
        final List<GSMCallFailureTerminalCauseCodeAnalysisResult> actualResults = getTranslator().translateResult(json,
                GSMCallFailureTerminalCauseCodeAnalysisResult.class);

        assertEquals(expectedResults, actualResults);
    }

    private void insertIrrelevantDataForOneDayQuery(final String categoryId) throws SQLException {
        //event with irrelevant time
        final String dateTimeNowMinus46Hours = DateTimeUtilities.getDateTimeMinusHours(46);
        insertRowToRaw(TEST_VALUE_CAUSE_GROUP_1, SAMPLE_TAC, dateTimeNowMinus46Hours, categoryId);
        insertRowToAgg(SAMPLE_TAC, TEST_VALUE_CAUSE_GROUP_1, 1, dateTimeNowMinus46Hours, categoryId);

        //event with irrelevant controller 
        final String dateTimeNowMinus27Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(27 + GSM_CFA_LATENCY_ON_ONE_DAY_QUERY);
        insertRowToRaw(TEST_VALUE_CAUSE_GROUP_1, SAMPLE_TAC, dateTimeNowMinus27Mins, categoryId);
        insertRowToAgg(SAMPLE_TAC_2, TEST_VALUE_CAUSE_GROUP_1, 1, dateTimeNowMinus27Mins, categoryId);

    }

    private List<GSMCallFailureTerminalCauseCodeAnalysisResult> insertEventDataAndGetExpectedResult(
            final int expectedTac, final String relevantCategoryId, final String irrelevantCategoryId) throws Exception {
        final String dateTimeNowMinus27Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(27 + GSM_CFA_LATENCY_ON_ONE_DAY_QUERY);
        final String dateTimeNowMinus26Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(26 + GSM_CFA_LATENCY_ON_ONE_DAY_QUERY);
        final String dateTimeNowMinus25Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(25 + GSM_CFA_LATENCY_ON_ONE_DAY_QUERY);

        final List<GSMCallFailureTerminalCauseCodeAnalysisResult> expectedResult = new ArrayList<GSMCallFailureTerminalCauseCodeAnalysisResult>();
        expectedResult.add(insertDataToDBAndGetExpectedResult(TEST_VALUE_CAUSE_GROUP_1_DESC, TEST_VALUE_CAUSE_GROUP_1,
                expectedTac, dateTimeNowMinus27Mins, 3, relevantCategoryId, MANUFACTURER_FOR_SAMPLE_TAC_2,
                MODEL_NO));
        expectedResult.add(insertDataToDBAndGetExpectedResult(TEST_VALUE_CAUSE_GROUP_2_DESC, TEST_VALUE_CAUSE_GROUP_2,
                expectedTac, dateTimeNowMinus26Mins, 2, relevantCategoryId, MANUFACTURER_FOR_SAMPLE_TAC_2,
                MODEL_NO));
        expectedResult.add(insertDataToDBAndGetExpectedResult(TEST_VALUE_CAUSE_GROUP_3_DESC, TEST_VALUE_CAUSE_GROUP_3,
                expectedTac, dateTimeNowMinus25Mins, 1, relevantCategoryId, MANUFACTURER_FOR_SAMPLE_TAC_2,
                MODEL_NO));
        
        //Populate data for irrelavant category id, in case of call drop put some call setup data also and vice versa
        insertRowToRaw(TEST_VALUE_CAUSE_GROUP_1, expectedTac, dateTimeNowMinus27Mins, irrelevantCategoryId);
        insertRowToRaw(TEST_VALUE_CAUSE_GROUP_2, expectedTac, dateTimeNowMinus26Mins, irrelevantCategoryId);
        insertRowToRaw(TEST_VALUE_CAUSE_GROUP_3, expectedTac, dateTimeNowMinus25Mins, irrelevantCategoryId);
        insertRowToAgg(expectedTac, TEST_VALUE_CAUSE_GROUP_1, 1, dateTimeNowMinus27Mins, irrelevantCategoryId);
        insertRowToAgg(expectedTac, TEST_VALUE_CAUSE_GROUP_2, 1, dateTimeNowMinus26Mins, irrelevantCategoryId);
        insertRowToAgg(expectedTac, TEST_VALUE_CAUSE_GROUP_3, 1, dateTimeNowMinus25Mins, irrelevantCategoryId);

        return expectedResult;
    }

    private GSMCallFailureTerminalCauseCodeAnalysisResult insertDataToDBAndGetExpectedResult(
            final String causeCodeDesc, final String causeCodeId, final int tac, final String time,
            final int noInstances, final String categoryId, final String Manuf, final String model) throws SQLException {
        for (int i = 0; i < noInstances; i++) {
            insertRowToRaw(causeCodeId, tac, time, categoryId);
        }
        insertRowToAgg(tac, causeCodeId, noInstances, time, categoryId);
        return getExpectedResult(causeCodeDesc, causeCodeId, noInstances, 1, tac, categoryId, Manuf, model);
    }

    private GSMCallFailureTerminalCauseCodeAnalysisResult getExpectedResult(final String causeCodeDesc,
            final String causeCodeId, final int numFailures, final int numImpactedSubs, final int tac,
            final String categoryId, final String Manuf, final String model) {
        final GSMCallFailureTerminalCauseCodeAnalysisResult expectedResult = new GSMCallFailureTerminalCauseCodeAnalysisResult();
        expectedResult.setCauseCodeDesc(causeCodeDesc);
        expectedResult.setCauseCodeId(causeCodeId);
        expectedResult.setNumFailures(numFailures);
        expectedResult.setNumImpactedSubs(numImpactedSubs);
        expectedResult.setTac(tac);
        expectedResult.setCategoryId(categoryId);
        expectedResult.setManufacturer(Manuf);
        expectedResult.setModel(model);
        expectedResult.setEventType(CALL_DROP);

        return expectedResult;
    }

    private void insertRowToRaw(final String urgencyCondition, final int tac, final String time, 
    		final String categoryId) throws SQLException {
        final Map<String, Object> dataForEventTable = new HashMap<String, Object>();
        dataForEventTable.put(TAC, tac);
        dataForEventTable.put(ApplicationTestConstants.CAUSE_GROUP, urgencyCondition);
        dataForEventTable.put(DATETIME_ID, time);
        dataForEventTable.put(IMSI, TEST_VALUE_IMSI);
        dataForEventTable.put(CATEGORY_ID, categoryId);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataForEventTable);
    }

    private void insertRowToAgg(final int tac, final String urgencyCondition, final int noFailures, 
    		final String time, final String categoryId)   throws SQLException {
        final Map<String, Object> dataForEventTable = new HashMap<String, Object>();
        //dataForEventTable.put(HIER3_ID, controllerHashId);
        dataForEventTable.put(TAC, tac);
        dataForEventTable.put(NO_OF_ERRORS, noFailures);
        dataForEventTable.put(ApplicationTestConstants.CAUSE_GROUP, urgencyCondition);
        dataForEventTable.put(DATETIME_ID, time);
        dataForEventTable.put(CATEGORY_ID, categoryId);
        insertRow(TEMP_EVENT_E_GSM_CFA_TAC_CG_EC_ERR_15MIN, dataForEventTable);
    }

    private void createLookupTables() throws Exception {
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_CAUSE_GROUP, "DIM_E_GSM_CFA_CAUSE_GROUP",
                ApplicationTestConstants.CAUSE_GROUP, CAUSE_GROUP_DESC);
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
        valuesForTable.put(ApplicationTestConstants.CAUSE_GROUP, urgencyCondition);
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

        columnsForRawTable.add(TAC);
        columnsForRawTable.add(ApplicationTestConstants.CAUSE_GROUP);
        columnsForRawTable.add(DATETIME_ID);
        columnsForRawTable.add(IMSI);
        columnsForRawTable.add(CATEGORY_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columnsForRawTable);

        final Collection<String> columnsForAggTable = new ArrayList<String>();
        columnsForAggTable.add(TAC);
        columnsForAggTable.add(ApplicationTestConstants.CAUSE_GROUP);
        columnsForAggTable.add(DATETIME_ID);
        columnsForAggTable.add(NO_OF_ERRORS);
        columnsForAggTable.add(CATEGORY_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_TAC_CG_EC_ERR_15MIN, columnsForAggTable);
    }

}
