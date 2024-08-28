/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.callfailure.cc;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.CATEGORY_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.CAUSE_CODE_DESCRIPTION;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.CAUSE_CODE_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DATETIME_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DISPLAY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.FAILURE_TYPE_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.GRID_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.GSM_CALL_DROP_CATEGORY_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.GSM_CALL_SETUP_FAILURE_CATEGORY_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.IMSI;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TAC;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TAC_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_MAN;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TYPE_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.resources.gsm.GSMApplicationConstants.MODEL;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.CALL_DROP;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.CAUSE_GROUP;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.EXCLUSIVE_TAC_GROUP;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.EXTENDED_CAUSE;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.EXTENDED_CAUSE_DESC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GROUP_NAME;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GSM_CALL_SETUP_FAILURES_CATEGORY_ID_DESC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GSM_CFA_LATENCY_ON_ONE_DAY_QUERY;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.MANUFACTURER_FOR_SAMPLE_TAC_2;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.MODEL_NO;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.NO_OF_ERRORS;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.ONE_DAY;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_EXCLUSIVE_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TEST_VALUE_EVENT_TYPE_CALL_SETUP;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TEST_VALUE_IMSI;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.THIRTY_MINUTES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TZ_OFFSET_OF_ZERO;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_DIM_E_GSM_CFA_EXTENDED_CAUSE;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_GSM_CFA_ERR_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_GSM_CFA_TAC_CG_EC_ERR_15MIN;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_GROUP_TYPE_E_TAC;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.cc.TerminalSubCCService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.GSMCallFailureTerminalSubCauseCodeAnalysisResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ekumkdn
 * @since 2012
 *
 */

public class GSMCallFailureTerminalSubCauseCodeAnalysisAggTest extends
        BaseDataIntegrityTest<GSMCallFailureTerminalSubCauseCodeAnalysisResult> {

    private static final String TEST_VALUE_CAUSE_GROUP_1 = "1";

    private static final String TEST_VALUE_CAUSE_GROUP_1_DESC = "EXCESSIVE TA";

    /* private static final String TEST_VALUE_CAUSE_GROUP_2 = "2";

     private static final String TEST_VALUE_CAUSE_GROUP_2_DESC = "SUDDENLY LOST CONNECTION";*/
    private static final String TEST_VALUE_CAUSE_GROUP_4 = "4";

    private static final String TEST_VALUE_CAUSE_GROUP_4_DESC = "DUMMY";

    private static final String TEST_VALUE_EXTENDED_CAUSE_1 = "1";

    private static final String TEST_VALUE_EXTENDED_CAUSE_1_DESC = "A-INTERFACE, TERRESTRIAL RESOURCE UNAVAILABLE";

    private static final String TEST_VALUE_EXTENDED_CAUSE_2 = "2";

    private static final String TEST_VALUE_EXTENDED_CAUSE_2_DESC = "A-INTERFACE, TERRESTRIAL RESOURCE ALLOCATED";

    private static final String TEST_VALUE_EXTENDED_CAUSE_3 = "3";

    private static final String TEST_VALUE_EXTENDED_CAUSE_3_DESC = "A-INTERFACE, SCCP DISCONNECTION INDICATION";


    private TerminalSubCCService service;

    @Before
    public void setup() throws Exception {
        service = new TerminalSubCCService();

        attachDependencies(service);
        createEventTables();
        createLookupTables();
    }

    @Test
    public void testOneDayQueryCallDrop() throws URISyntaxException, Exception {
        insertAllLookupData();
        final List<GSMCallFailureTerminalSubCauseCodeAnalysisResult> expectedResult = insertEventDataAndGetExpectedResult(
        		GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER,GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER);
        final String actualJSONResultStr = setParamsAndRunQuery(ONE_DAY, TEST_VALUE_CAUSE_GROUP_1,
                TEST_VALUE_CAUSE_GROUP_1_DESC, GSM_CALL_DROP_CATEGORY_ID, CALL_DROP);
        verifyResult(actualJSONResultStr, expectedResult);
    }

    @Test
    public void testOneDayQueryWithNoMatchingCauseCodeCallDrop() throws URISyntaxException, Exception {
        insertAllLookupData();
        insertEventDataAndGetExpectedResult(GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER,GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER);
        final String actualJSONResultStr = setParamsAndRunQuery(THIRTY_MINUTES, TEST_VALUE_CAUSE_GROUP_4,
                TEST_VALUE_CAUSE_GROUP_4_DESC, GSM_CALL_DROP_CATEGORY_ID, CALL_DROP);
        verifyResultIsEmpty(actualJSONResultStr);
    }

    @Test
    public void testOneDayQueryWithIrrelevantDataCallDrop() throws URISyntaxException, Exception {
        insertAllLookupData();
        final List<GSMCallFailureTerminalSubCauseCodeAnalysisResult> expectedResult = insertEventDataAndGetExpectedResult(
        		GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER,GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER);
        insertIrrelevantDataForOneDayQuery(GSM_CALL_DROP_CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID);
        final String actualJSONResultStr = setParamsAndRunQuery(ONE_DAY, TEST_VALUE_CAUSE_GROUP_1,
                TEST_VALUE_CAUSE_GROUP_1_DESC, GSM_CALL_DROP_CATEGORY_ID, CALL_DROP);
        verifyResult(actualJSONResultStr, expectedResult);
    }

    @Test
    public void testOneDayQueryWithExclusiveTACCallDrop() throws URISyntaxException, Exception {
        insertAllLookupData();
        final List<GSMCallFailureTerminalSubCauseCodeAnalysisResult> expectedResult = new ArrayList<GSMCallFailureTerminalSubCauseCodeAnalysisResult>();
        boolean skipAggrDataInsertion = true;
        insertEventDataAndGetExpectedResult(skipAggrDataInsertion, 
        		GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER,GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER);

        insertTacIntoExclusiveTacGroup(SAMPLE_TAC);
        final String actualJSONResultStr = setParamsAndRunQuery(ONE_DAY, TEST_VALUE_CAUSE_GROUP_1,
                TEST_VALUE_CAUSE_GROUP_1_DESC, GSM_CALL_DROP_CATEGORY_ID, CALL_DROP);
        verifyResult(actualJSONResultStr, expectedResult);
    }

    @Test
    public void testOneDayQueryCallSetup() throws URISyntaxException, Exception {
        insertAllLookupData();
        final List<GSMCallFailureTerminalSubCauseCodeAnalysisResult> expectedResult = insertEventDataAndGetExpectedResult(
        		GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        final String actualJSONResultStr = setParamsAndRunQuery(ONE_DAY, TEST_VALUE_CAUSE_GROUP_1,
                TEST_VALUE_CAUSE_GROUP_1_DESC, GSM_CALL_SETUP_FAILURE_CATEGORY_ID, TEST_VALUE_EVENT_TYPE_CALL_SETUP);
        verifyResult(actualJSONResultStr, expectedResult);
    }

    @Test
    public void testOneDayQueryWithNoMatchingCauseCodeCallSetup() throws URISyntaxException, Exception {
        insertAllLookupData();
        insertEventDataAndGetExpectedResult(GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        final String actualJSONResultStr = setParamsAndRunQuery(THIRTY_MINUTES, TEST_VALUE_CAUSE_GROUP_4,
                TEST_VALUE_CAUSE_GROUP_4_DESC, GSM_CALL_SETUP_FAILURE_CATEGORY_ID, TEST_VALUE_EVENT_TYPE_CALL_SETUP);
        verifyResultIsEmpty(actualJSONResultStr);
    }

    @Test
    public void testOneDayQueryWithIrrelevantDataCallSetup() throws URISyntaxException, Exception {
        insertAllLookupData();
        final List<GSMCallFailureTerminalSubCauseCodeAnalysisResult> expectedResult = insertEventDataAndGetExpectedResult(
        		GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertIrrelevantDataForOneDayQuery(GSM_CALL_SETUP_FAILURE_CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID);
        final String actualJSONResultStr = setParamsAndRunQuery(ONE_DAY, TEST_VALUE_CAUSE_GROUP_1,
                TEST_VALUE_CAUSE_GROUP_1_DESC, GSM_CALL_SETUP_FAILURE_CATEGORY_ID, TEST_VALUE_EVENT_TYPE_CALL_SETUP);
        verifyResult(actualJSONResultStr, expectedResult);
    }

    @Test
    public void testOneDayQueryWithExclusiveTACCallSetup() throws URISyntaxException, Exception {
        insertAllLookupData();
        final List<GSMCallFailureTerminalSubCauseCodeAnalysisResult> expectedResult = new ArrayList<GSMCallFailureTerminalSubCauseCodeAnalysisResult>();
        boolean skipAggrDataInsertion = true;
        insertEventDataAndGetExpectedResult(skipAggrDataInsertion, 
        		GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);

        insertTacIntoExclusiveTacGroup(SAMPLE_TAC);
        final String actualJSONResultStr = setParamsAndRunQuery(ONE_DAY, TEST_VALUE_CAUSE_GROUP_1,
                TEST_VALUE_CAUSE_GROUP_1_DESC, GSM_CALL_SETUP_FAILURE_CATEGORY_ID, TEST_VALUE_EVENT_TYPE_CALL_SETUP);
        verifyResult(actualJSONResultStr, expectedResult);
    }

    private String setParamsAndRunQuery(final String mins, final String causeCodeId, final String causeCodeDescription,
    		final String categoryId, final String eventType) {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TIME_QUERY_PARAM, mins);
        requestParameters.add(TYPE_MAN, MANUFACTURER_FOR_SAMPLE_TAC_2);
        requestParameters.add(MODEL, MODEL_NO);
        requestParameters.add(TAC_PARAM, "101800");
        requestParameters.add(TYPE_PARAM, TAC);
        requestParameters.add(CATEGORY_ID, categoryId);
        requestParameters.add(FAILURE_TYPE_PARAM, eventType);

        requestParameters.add(CAUSE_CODE_PARAM, causeCodeId);
        requestParameters.add(CAUSE_CODE_DESCRIPTION, causeCodeDescription);
        requestParameters.add(DISPLAY_PARAM, GRID_PARAM);

        return runQuery(service, requestParameters);
    }

    private void verifyResult(final String json,
            final List<GSMCallFailureTerminalSubCauseCodeAnalysisResult> expectedResults) throws Exception {
        assertJSONSucceeds(json);
        validateAgainstGridDefinition(json, "GSM_SUB_CAUSE_CODE_ANALYSIS_BY_TAC_EVENT_TYPE");
        final List<GSMCallFailureTerminalSubCauseCodeAnalysisResult> actualResults = getTranslator().translateResult(
                json, GSMCallFailureTerminalSubCauseCodeAnalysisResult.class);

        assertEquals(expectedResults, actualResults);
    }

    private void verifyResultIsEmpty(final String json) throws Exception {
        assertJSONSucceeds(json);
        final List<GSMCallFailureTerminalSubCauseCodeAnalysisResult> actualResults = getTranslator().translateResult(
                json, GSMCallFailureTerminalSubCauseCodeAnalysisResult.class);

        assertThat(actualResults.size(), is(0));
    }

    /*private void tweakExpectedResultForExclusiveTACs(
            final List<GSMCallFailureTerminalSubCauseCodeAnalysisResult> expectedResult) {
        //TAC in the exclusive tac group means that although the event is in the agg data, it should not be used in 
        //  calculations for impacted subscribers
        expectedResult.get(2).setNumImpactedSubs(0);
    }*/

    private void insertIrrelevantDataForOneDayQuery(final String relCategoryId, final String irrelCategoryId) throws SQLException {
        //event with irrelevant time
        final String dateTimeNowMinus46Hours = DateTimeUtilities.getDateTimeMinusHours(46);
        insertRowToRaw(TEST_VALUE_EXTENDED_CAUSE_1, SAMPLE_TAC, dateTimeNowMinus46Hours, relCategoryId);
        insertRowToAgg(TEST_VALUE_EXTENDED_CAUSE_1, 1, dateTimeNowMinus46Hours, SAMPLE_TAC, relCategoryId);

        
        insertRowToRaw(TEST_VALUE_EXTENDED_CAUSE_1, SAMPLE_TAC, dateTimeNowMinus46Hours, irrelCategoryId);
        insertRowToAgg(TEST_VALUE_EXTENDED_CAUSE_1, 1, dateTimeNowMinus46Hours, SAMPLE_TAC, irrelCategoryId);
        //unused TAC in EXCLUSIVE_TAC group
        insertTacIntoExclusiveTacGroup(SAMPLE_EXCLUSIVE_TAC);
    }

    private List<GSMCallFailureTerminalSubCauseCodeAnalysisResult> insertEventDataAndGetExpectedResult(
            boolean skipAggrDataInsert, final int relevantCategoryId,final int irrelavantCategoryId) throws Exception {

        final String dateTimeNowMinus27Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(27 + GSM_CFA_LATENCY_ON_ONE_DAY_QUERY);
        final String dateTimeNowMinus26Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(26 + GSM_CFA_LATENCY_ON_ONE_DAY_QUERY);
        final String dateTimeNowMinus25Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(25 + GSM_CFA_LATENCY_ON_ONE_DAY_QUERY);

        final List<GSMCallFailureTerminalSubCauseCodeAnalysisResult> expectedResult = new ArrayList<GSMCallFailureTerminalSubCauseCodeAnalysisResult>();

        expectedResult.add(insertDataToDBAndGetExpectedResult(TEST_VALUE_EXTENDED_CAUSE_1_DESC,
                TEST_VALUE_EXTENDED_CAUSE_1, SAMPLE_TAC, dateTimeNowMinus27Mins, 3, skipAggrDataInsert, relevantCategoryId));
        expectedResult.add(insertDataToDBAndGetExpectedResult(TEST_VALUE_EXTENDED_CAUSE_2_DESC,
                TEST_VALUE_EXTENDED_CAUSE_2, SAMPLE_TAC, dateTimeNowMinus26Mins, 2, skipAggrDataInsert, relevantCategoryId));
        expectedResult.add(insertDataToDBAndGetExpectedResult(TEST_VALUE_EXTENDED_CAUSE_3_DESC,
                TEST_VALUE_EXTENDED_CAUSE_3, SAMPLE_TAC, dateTimeNowMinus25Mins, 1, skipAggrDataInsert, relevantCategoryId));

        insertRowToRaw(TEST_VALUE_EXTENDED_CAUSE_1, SAMPLE_TAC, dateTimeNowMinus27Mins,""+irrelavantCategoryId);
        insertRowToRaw(TEST_VALUE_EXTENDED_CAUSE_2, SAMPLE_TAC, dateTimeNowMinus26Mins,""+irrelavantCategoryId);
        insertRowToRaw(TEST_VALUE_EXTENDED_CAUSE_3, SAMPLE_TAC, dateTimeNowMinus25Mins,""+irrelavantCategoryId);
        
        if(!skipAggrDataInsert) {
        	 insertRowToAgg(TEST_VALUE_EXTENDED_CAUSE_1, 1, dateTimeNowMinus27Mins, SAMPLE_TAC,""+irrelavantCategoryId);
        	 insertRowToAgg(TEST_VALUE_EXTENDED_CAUSE_2, 1, dateTimeNowMinus26Mins, SAMPLE_TAC,""+irrelavantCategoryId);
        	 insertRowToAgg(TEST_VALUE_EXTENDED_CAUSE_3, 1, dateTimeNowMinus25Mins, SAMPLE_TAC,""+irrelavantCategoryId);
        }
        
        return expectedResult;

    }

    private List<GSMCallFailureTerminalSubCauseCodeAnalysisResult> insertEventDataAndGetExpectedResult(final int relevantCategoryId,
    		final int irrelavantCategoryId)    throws Exception {
        return insertEventDataAndGetExpectedResult(false, relevantCategoryId, irrelavantCategoryId);
    }

    private GSMCallFailureTerminalSubCauseCodeAnalysisResult insertDataToDBAndGetExpectedResult(
            final String subCauseCodeDesc, final String subCauseCodeId, final int tac, final String time,
            final int noInstances, boolean skipAggrInsert, final int categoryId) throws SQLException {

        for (int i = 0; i < noInstances; i++) {
            insertRowToRaw(subCauseCodeId, tac, time,""+categoryId);
        }
        if (!skipAggrInsert)
            insertRowToAgg(subCauseCodeId, noInstances, time, tac,""+categoryId);
        return getExpectedResult(subCauseCodeDesc, subCauseCodeId, noInstances, 1, categoryId);

    }

    /*    private GSMCallFailureTerminalSubCauseCodeAnalysisResult insertDataToDBAndGetExpectedResult(
                final String subCauseCodeDesc, final String subCauseCodeId, final int tac, final String time,
                final int noInstances) throws SQLException {
            return insertDataToDBAndGetExpectedResult(subCauseCodeDesc, subCauseCodeId, tac, time, noInstances, false);
        }*/

    private GSMCallFailureTerminalSubCauseCodeAnalysisResult getExpectedResult(final String subCauseCodeDesc,
            final String subCauseCodeId, final int numFailures, final int numImpactedSubs, final int categoryID) {
        final GSMCallFailureTerminalSubCauseCodeAnalysisResult expectedResult = new GSMCallFailureTerminalSubCauseCodeAnalysisResult();
        expectedResult.setSubCauseCodeDesc(subCauseCodeDesc);
        expectedResult.setSubCauseCodeId(subCauseCodeId);
        expectedResult.setNumFailures(numFailures);
        expectedResult.setNumImpactedSubs(numImpactedSubs);
        expectedResult.setCategoryId(categoryID);
        return expectedResult;
    }

    private void insertRowToRaw(final String extendedCause, final int tac, final String time, final String categoryId) throws SQLException {
        final Map<String, Object> dataForEventTable = new HashMap<String, Object>();

        dataForEventTable.put(TAC, tac);
        dataForEventTable.put(CAUSE_GROUP, TEST_VALUE_CAUSE_GROUP_1);
        dataForEventTable.put(EXTENDED_CAUSE, extendedCause);
        dataForEventTable.put(DATETIME_ID, time);
        dataForEventTable.put(IMSI, TEST_VALUE_IMSI);
        dataForEventTable.put(CATEGORY_ID, categoryId);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataForEventTable);
    }

    private void insertRowToAgg(final String extendedCause, final int noFailures, final String time, final int tac, final String categoryId)
            throws SQLException {
        final Map<String, Object> dataForEventTable = new HashMap<String, Object>();

        dataForEventTable.put(TAC, tac);
        dataForEventTable.put(NO_OF_ERRORS, noFailures);
        dataForEventTable.put(CAUSE_GROUP, TEST_VALUE_CAUSE_GROUP_1);
        dataForEventTable.put(EXTENDED_CAUSE, extendedCause);
        dataForEventTable.put(DATETIME_ID, time);
        dataForEventTable.put(CATEGORY_ID, categoryId);
        insertRow(TEMP_EVENT_E_GSM_CFA_TAC_CG_EC_ERR_15MIN, dataForEventTable);
    }

    private void createLookupTables() throws Exception {
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_EXTENDED_CAUSE, "DIM_E_GSM_CFA_EXTENDED_CAUSE", EXTENDED_CAUSE,
                EXTENDED_CAUSE_DESC);
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
        insertExtendedCauseLookupData();
    }

    private void insertRowToUrgencyConditionTable(final String urgencyCondition,
            final String urgencyConditionDescription) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(EXTENDED_CAUSE, urgencyCondition);
        valuesForTable.put(EXTENDED_CAUSE_DESC, urgencyConditionDescription);
        insertRow(TEMP_DIM_E_GSM_CFA_EXTENDED_CAUSE, valuesForTable);
    }

    private void insertExtendedCauseLookupData() throws SQLException {
        insertRowToUrgencyConditionTable(TEST_VALUE_EXTENDED_CAUSE_1, TEST_VALUE_EXTENDED_CAUSE_1_DESC);
        insertRowToUrgencyConditionTable(TEST_VALUE_EXTENDED_CAUSE_2, TEST_VALUE_EXTENDED_CAUSE_2_DESC);
        insertRowToUrgencyConditionTable(TEST_VALUE_EXTENDED_CAUSE_3, TEST_VALUE_EXTENDED_CAUSE_3_DESC);
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
        columnsForRawTable.add(CAUSE_GROUP);
        columnsForRawTable.add(EXTENDED_CAUSE);
        columnsForRawTable.add(DATETIME_ID);
        columnsForRawTable.add(IMSI);
        columnsForRawTable.add(CATEGORY_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columnsForRawTable);

        final Collection<String> columnsForAggTable = new ArrayList<String>();

        columnsForAggTable.add(TAC);
        columnsForAggTable.add(CAUSE_GROUP);
        columnsForAggTable.add(EXTENDED_CAUSE);
        columnsForAggTable.add(DATETIME_ID);
        columnsForAggTable.add(NO_OF_ERRORS);
        columnsForAggTable.add(CATEGORY_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_TAC_CG_EC_ERR_15MIN, columnsForAggTable);
    }

}
