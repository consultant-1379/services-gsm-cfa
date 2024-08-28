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
import static com.ericsson.eniq.events.server.common.ApplicationConstants.FAILURE_TYPE_PARAM;
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
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TEST_VALUE_EVENT_TYPE_CALL_SETUP;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.CAUSE_GROUP;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.EXCLUSIVE_TAC_GROUP;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.EXTENDED_CAUSE;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.EXTENDED_CAUSE_DESC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GROUP_NAME;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.GSM_CFA_LATENCY_ON_THIRTY_MIN_QUERY;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.MANUFACTURER_FOR_SAMPLE_TAC_2;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.MODEL_NO;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_EXCLUSIVE_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.SAMPLE_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TEST_VALUE_IMSI;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.THIRTY_MINUTES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TZ_OFFSET_OF_ZERO;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_DIM_E_GSM_CFA_EXTENDED_CAUSE;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_GSM_CFA_ERR_RAW;
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
 * @author ekumjay
 * @since 2012
 *
 */

public class GSMCallFailureTerminalSubCauseCodeAnalysisRawTest extends
        BaseDataIntegrityTest<GSMCallFailureTerminalSubCauseCodeAnalysisResult> {

    private static final String TEST_VALUE_EXTENDED_CAUSE_1 = "1";

    private static final String TEST_VALUE_EXTENDED_CAUSE_1_DESC = "A-INTERFACE, TERRESTRIAL RESOURCE UNAVAILABLE";

    private static final String TEST_VALUE_EXTENDED_CAUSE_2 = "2";

    private static final String TEST_VALUE_EXTENDED_CAUSE_2_DESC = "A-INTERFACE, TERRESTRIAL RESOURCE ALLOCATED";

    private static final String TEST_VALUE_EXTENDED_CAUSE_3 = "3";

    private static final String TEST_VALUE_EXTENDED_CAUSE_3_DESC = "A-INTERFACE, SCCP DISCONNECTION INDICATION";

    private static final String TEST_VALUE_CAUSE_GROUP_1 = "1";

    private static final String TEST_VALUE_CAUSE_GROUP_1_DESC = "EXCESSIVE TA";

    private static final String TEST_VALUE_CAUSE_GROUP_2 = "2";

    private static final String TEST_VALUE_CAUSE_GROUP_2_DESC = "SUDDENLY LOST CONNECTION";

    private TerminalSubCCService service;

    @Before
    public void setup() throws Exception {
        service = new TerminalSubCCService();

        attachDependencies(service);
        createEventTable();
        createLookupTables();
    }

    @Test
    public void testThirtyMinuteQueryCallDrop() throws URISyntaxException, Exception {
        insertAllLookupData();
        final List<GSMCallFailureTerminalSubCauseCodeAnalysisResult> expectedResult = insertEventDataAndGetExpectedResult(SAMPLE_TAC, 
        		GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER,GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER );
        final String actualJSONResultStr = setParamsAndRunQuery(THIRTY_MINUTES, TEST_VALUE_CAUSE_GROUP_1,
                TEST_VALUE_CAUSE_GROUP_1_DESC, GSM_CALL_DROP_CATEGORY_ID, CALL_DROP);
        verifyResult(actualJSONResultStr, expectedResult);
    }

    @Test
    public void testThirtyMinuteQueryWithNoMatchingCauseCodeCallDrop() throws URISyntaxException, Exception {
        insertAllLookupData();
        insertEventDataAndGetExpectedResult(SAMPLE_TAC, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER,GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER);
        final String actualJSONResultStr = setParamsAndRunQuery(THIRTY_MINUTES, TEST_VALUE_CAUSE_GROUP_2,
                TEST_VALUE_CAUSE_GROUP_2_DESC, GSM_CALL_DROP_CATEGORY_ID, CALL_DROP);
        verifyResultIsEmpty(actualJSONResultStr);
    }

    @Test
    public void testThirtyMinuteQueryWithIrrelevantDataCallDrop() throws URISyntaxException, Exception {
        insertAllLookupData();
        final List<GSMCallFailureTerminalSubCauseCodeAnalysisResult> expectedResult = insertEventDataAndGetExpectedResult(SAMPLE_TAC,
        		GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER,GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER);
        insertIrrelevantDataForThirtyMinsQuery(GSM_CALL_DROP_CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID);
        final String actualJSONResultStr = setParamsAndRunQuery(THIRTY_MINUTES, TEST_VALUE_CAUSE_GROUP_1,
                TEST_VALUE_CAUSE_GROUP_1_DESC, GSM_CALL_DROP_CATEGORY_ID, CALL_DROP);
        verifyResult(actualJSONResultStr, expectedResult);
    }

    @Test
    public void testThirtyMinuteQueryWithExclusiveTACCallDrop() throws URISyntaxException, Exception {
        insertAllLookupData();
        final List<GSMCallFailureTerminalSubCauseCodeAnalysisResult> expectedResult = new ArrayList<GSMCallFailureTerminalSubCauseCodeAnalysisResult>();
        insertEventDataAndGetExpectedResult(SAMPLE_TAC,GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER,GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER);

        insertTacIntoExclusiveTacGroup(SAMPLE_TAC);
        final String actualJSONResultStr = setParamsAndRunQuery(THIRTY_MINUTES, TEST_VALUE_CAUSE_GROUP_1,
                TEST_VALUE_CAUSE_GROUP_1_DESC, GSM_CALL_DROP_CATEGORY_ID, CALL_DROP);
        verifyResult(actualJSONResultStr, expectedResult);
    }
    
    @Test
    public void testThirtyMinuteQueryCallSetup() throws URISyntaxException, Exception {
        insertAllLookupData();
        final List<GSMCallFailureTerminalSubCauseCodeAnalysisResult> expectedResult = insertEventDataAndGetExpectedResult(SAMPLE_TAC, 
        		GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER );
        final String actualJSONResultStr = setParamsAndRunQuery(THIRTY_MINUTES, TEST_VALUE_CAUSE_GROUP_1,
                TEST_VALUE_CAUSE_GROUP_1_DESC, GSM_CALL_SETUP_FAILURE_CATEGORY_ID, TEST_VALUE_EVENT_TYPE_CALL_SETUP);
        verifyResult(actualJSONResultStr, expectedResult);
    }

    @Test
    public void testThirtyMinuteQueryWithNoMatchingCauseCodeCallSetup() throws URISyntaxException, Exception {
        insertAllLookupData();
        insertEventDataAndGetExpectedResult(SAMPLE_TAC, GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        final String actualJSONResultStr = setParamsAndRunQuery(THIRTY_MINUTES, TEST_VALUE_CAUSE_GROUP_2,
                TEST_VALUE_CAUSE_GROUP_2_DESC, GSM_CALL_SETUP_FAILURE_CATEGORY_ID, TEST_VALUE_EVENT_TYPE_CALL_SETUP);
        verifyResultIsEmpty(actualJSONResultStr);
    }

    @Test
    public void testThirtyMinuteQueryWithIrrelevantDataCallSetup() throws URISyntaxException, Exception {
        insertAllLookupData();
        final List<GSMCallFailureTerminalSubCauseCodeAnalysisResult> expectedResult = insertEventDataAndGetExpectedResult(SAMPLE_TAC,
        		GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER,GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertIrrelevantDataForThirtyMinsQuery(GSM_CALL_SETUP_FAILURE_CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID );
        final String actualJSONResultStr = setParamsAndRunQuery(THIRTY_MINUTES, TEST_VALUE_CAUSE_GROUP_1,
                TEST_VALUE_CAUSE_GROUP_1_DESC, GSM_CALL_SETUP_FAILURE_CATEGORY_ID, TEST_VALUE_EVENT_TYPE_CALL_SETUP);
        verifyResult(actualJSONResultStr, expectedResult);
    }

    @Test
    public void testThirtyMinuteQueryWithExclusiveTACCallSetup() throws URISyntaxException, Exception {
        insertAllLookupData();
        final List<GSMCallFailureTerminalSubCauseCodeAnalysisResult> expectedResult = new ArrayList<GSMCallFailureTerminalSubCauseCodeAnalysisResult>();
        insertEventDataAndGetExpectedResult(SAMPLE_TAC,GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER,GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);

        insertTacIntoExclusiveTacGroup(SAMPLE_TAC);
        final String actualJSONResultStr = setParamsAndRunQuery(THIRTY_MINUTES, TEST_VALUE_CAUSE_GROUP_1,
                TEST_VALUE_CAUSE_GROUP_1_DESC, GSM_CALL_SETUP_FAILURE_CATEGORY_ID, TEST_VALUE_EVENT_TYPE_CALL_SETUP);
        verifyResult(actualJSONResultStr, expectedResult);
    }

    private String setParamsAndRunQuery(final String mins, final String causeCodeId, 
    		final String causeCodeDesc, final String categoryId, final String eventType) {

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
        requestParameters.add(CAUSE_CODE_DESCRIPTION, causeCodeDesc);

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

    private void insertIrrelevantDataForThirtyMinsQuery(final String relCategoryId, final String irrelCategoryId) throws SQLException {
        //event with irrelevant time
        final String dateTimeNowMinus37Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(37 + GSM_CFA_LATENCY_ON_THIRTY_MIN_QUERY);
        insertRowToRaw(TEST_VALUE_EXTENDED_CAUSE_1, SAMPLE_TAC, dateTimeNowMinus37Mins,relCategoryId);
        insertRowToRaw(TEST_VALUE_EXTENDED_CAUSE_1, SAMPLE_TAC, dateTimeNowMinus37Mins,irrelCategoryId);

        final String dateTimeNowMinus47Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(47 + GSM_CFA_LATENCY_ON_THIRTY_MIN_QUERY);
        insertRowToRaw(TEST_VALUE_EXTENDED_CAUSE_1, SAMPLE_TAC, dateTimeNowMinus47Mins, relCategoryId);
        insertRowToRaw(TEST_VALUE_EXTENDED_CAUSE_1, SAMPLE_TAC, dateTimeNowMinus47Mins, irrelCategoryId);

        //unused TAC in EXCLUSIVE_TAC group
        insertTacIntoExclusiveTacGroup(SAMPLE_EXCLUSIVE_TAC);
    }

    private List<GSMCallFailureTerminalSubCauseCodeAnalysisResult> insertEventDataAndGetExpectedResult(
            final int expectedTerminal, final int relevantCategoryId,final int irrelavantCategoryId) throws Exception {
        final String dateTimeNowMinus27Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(27 + GSM_CFA_LATENCY_ON_THIRTY_MIN_QUERY);
        final String dateTimeNowMinus26Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(26 + GSM_CFA_LATENCY_ON_THIRTY_MIN_QUERY);
        final String dateTimeNowMinus25Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(25 + GSM_CFA_LATENCY_ON_THIRTY_MIN_QUERY);

        final List<GSMCallFailureTerminalSubCauseCodeAnalysisResult> expectedResult = new ArrayList<GSMCallFailureTerminalSubCauseCodeAnalysisResult>();
        expectedResult.add(insertDataToDBAndGetExpectedResult(TEST_VALUE_EXTENDED_CAUSE_1_DESC,
                TEST_VALUE_EXTENDED_CAUSE_1, SAMPLE_TAC, dateTimeNowMinus27Mins, 3, relevantCategoryId));
        expectedResult.add(insertDataToDBAndGetExpectedResult(TEST_VALUE_EXTENDED_CAUSE_2_DESC,
                TEST_VALUE_EXTENDED_CAUSE_2, SAMPLE_TAC, dateTimeNowMinus26Mins, 2, relevantCategoryId));

        expectedResult.add(insertDataToDBAndGetExpectedResult(TEST_VALUE_EXTENDED_CAUSE_3_DESC,
                TEST_VALUE_EXTENDED_CAUSE_3, SAMPLE_TAC, dateTimeNowMinus25Mins, 1, relevantCategoryId));
        // Put some data for other category id
        insertRowToRaw(TEST_VALUE_EXTENDED_CAUSE_1, SAMPLE_TAC, dateTimeNowMinus27Mins,""+irrelavantCategoryId);
        insertRowToRaw(TEST_VALUE_EXTENDED_CAUSE_2, SAMPLE_TAC, dateTimeNowMinus26Mins,""+irrelavantCategoryId);
        insertRowToRaw(TEST_VALUE_EXTENDED_CAUSE_3, SAMPLE_TAC, dateTimeNowMinus25Mins,""+irrelavantCategoryId);
        return expectedResult;
    }

    private GSMCallFailureTerminalSubCauseCodeAnalysisResult insertDataToDBAndGetExpectedResult(
            final String subCauseCodeDesc, final String subCauseCodeId, final int tac, final String time,
            final int noInstances, final int categoryId) throws SQLException {
        for (int i = 0; i < noInstances; i++) {
            insertRowToRaw(subCauseCodeId, tac, time,""+categoryId);
        }
        return getExpectedResult(subCauseCodeDesc, subCauseCodeId, noInstances, 1, categoryId);
    }

    private GSMCallFailureTerminalSubCauseCodeAnalysisResult getExpectedResult(final String subCauseCodeDesc,
            final String subCauseCodeId, final int numFailures, final int numImpactedSubs, final int categoryId) {
        final GSMCallFailureTerminalSubCauseCodeAnalysisResult expectedResult = new GSMCallFailureTerminalSubCauseCodeAnalysisResult();
        expectedResult.setSubCauseCodeDesc(subCauseCodeDesc);
        expectedResult.setSubCauseCodeId(subCauseCodeId);
        expectedResult.setNumFailures(numFailures);
        expectedResult.setNumImpactedSubs(numImpactedSubs);
        expectedResult.setCategoryId(categoryId);
        return expectedResult;
    }

    private void insertRowToRaw(final String extendedCause, final int tac, final String time, final String categoryID) throws SQLException {
        final Map<String, Object> dataForEventTable = new HashMap<String, Object>();

        dataForEventTable.put(TAC, tac);
        dataForEventTable.put(CAUSE_GROUP, TEST_VALUE_CAUSE_GROUP_1);
        dataForEventTable.put(EXTENDED_CAUSE, extendedCause);
        dataForEventTable.put(DATETIME_ID, time);
        dataForEventTable.put(IMSI, TEST_VALUE_IMSI);
        dataForEventTable.put(CATEGORY_ID, categoryID);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataForEventTable);

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

    private void insertRowToExtendedCauseTable(final String extendedCause, final String extendedCauseDescription)
            throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(EXTENDED_CAUSE, extendedCause);
        valuesForTable.put(EXTENDED_CAUSE_DESC, extendedCauseDescription);
        insertRow(TEMP_DIM_E_GSM_CFA_EXTENDED_CAUSE, valuesForTable);
    }

    private void insertExtendedCauseLookupData() throws SQLException {
        insertRowToExtendedCauseTable(TEST_VALUE_EXTENDED_CAUSE_1, TEST_VALUE_EXTENDED_CAUSE_1_DESC);
        insertRowToExtendedCauseTable(TEST_VALUE_EXTENDED_CAUSE_2, TEST_VALUE_EXTENDED_CAUSE_2_DESC);
        insertRowToExtendedCauseTable(TEST_VALUE_EXTENDED_CAUSE_3, TEST_VALUE_EXTENDED_CAUSE_3_DESC);
    }

    private void insertTacIntoExclusiveTacGroup(final int tac) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, tac);
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    private void createEventTable() throws Exception {
        final Collection<String> columnsForEventTable = new ArrayList<String>();

        columnsForEventTable.add(TAC);
        columnsForEventTable.add(CAUSE_GROUP);
        columnsForEventTable.add(EXTENDED_CAUSE);
        columnsForEventTable.add(DATETIME_ID);
        columnsForEventTable.add(IMSI);
        columnsForEventTable.add(CATEGORY_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columnsForEventTable);
    }

}
