/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.callfailure.events;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.Matchers.*;
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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.TerminalDetailedService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.GSMCallFailureTerminalDetailedEventAnalysisResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ejoegaf
 * @since 2011
 * 
 */
public class GSMCFATerminalDetailedEventRawTest extends
        BaseDataIntegrityTest<GSMCallFailureTerminalDetailedEventAnalysisResult> {

    private static final String TEST_VALUE_EVENT_ID = GSM_CALL_DROP_CATEGORY_ID;

    private static final String TEST_VALUE_HIER321_ID = "4948639634796658772";

    private static final String TEST_VALUE_HIER3_ID = "5386564559998864911";

    private static final String TEST_VALUE_CAUSE_GROUP = "4";

    private static final String TEST_VALUE_CAUSE_GROUP_DESC = "LOW SS DOWNLINK";

    private static final String TEST_VALUE_EXTENDED_CAUSE = "61";

    private static final String TEST_VALUE_EXTENDED_DESC = "OTHER, PREEMPTION";

    private static final String TEST_VALUE_URGENCY_CONDITION = "1";

    private static final String TEST_VALUE_URGENCY_CONDITION_DESC = "Urgency1";

    private static final String TEST_VALUE_IMSI1 = "46000608201336";

    private static final int TEST_VALUE_TAC = 101800;

    private static final String TEST_VALUE_MANUFACTURER = "Mitsubishi";

    private static final String TEST_VALUE_MARKETING_NAME = "G410";

    private static final int TEST_VALUE_RELEASE_TYPE = 0;

    private static final String TEST_VALUE_RELEASE_TYPE_DESC = "(MSC) NORMAL RELEASE";

    private static final String TEST_VAMOS_NEIGHBOR_INDICATOR = "1";

    private static final String TEST_VAMOS_PAIR_ALLOCATION_BY_MS = "Neighbor1";

    private static final String TEST_RSAI = "1";

    private static final String TEST_RSAI_DESC = "RSAI1";

    private static final String TEST_CHANNEL_TYPE = "1";

    private static final String TEST_CHANNEL_TYPE_DESC = "Channel1";

    private static final String TEST_MSISDN = "123456";

    private static final String TEST_VALUE_VENDOR = "Ericsson";

    private TerminalDetailedService service;

    @Before
    public void setup() throws Exception {
        service = new TerminalDetailedService();

        attachDependencies(service);
        createEventTable();
        createLookupTables();
    }

    @Test
    public void testThirtyMinuteQuery() throws URISyntaxException, Exception {
        insertAllLookupData();
        final List<GSMCallFailureTerminalDetailedEventAnalysisResult> expectedResult = insertEventDataAndGetExpectedResult(
                TEST_VALUE_GSM_CONTROLLER1_NAME, TEST_VALUE_GSM_CELL1_NAME);
        final String actualJSONResultStr = setParamsAndRunQuery(THIRTY_MINUTES);
        // verifyResult(actualJSONResultStr, expectedResult);
    }

    @Test
    public void testThirtyMinuteQueryMissingHashIdLookupData() throws URISyntaxException, Exception {
        // note - there is no corresponding hashId in the lookup table. The
        // event should still be returned but the
        // controller and cell info is missing
        insertEventTypeLookupData();
        insertUrgencyConditionLookupData();
        insertExtendedCauseLookupData();
        insertReleaseTypeLookupData();
        final List<GSMCallFailureTerminalDetailedEventAnalysisResult> expectedResult = insertEventDataAndGetExpectedResult(
                "", "");
        final String actualJSONResultStr = setParamsAndRunQuery(THIRTY_MINUTES);
        // verifyResult(actualJSONResultStr, expectedResult);
    }

    @Test
    public void testThirtyMinuteQueryWithIrrelevantData() throws URISyntaxException, Exception {
        insertAllLookupData();
        final List<GSMCallFailureTerminalDetailedEventAnalysisResult> expectedResult = insertEventDataAndGetExpectedResult(
                TEST_VALUE_GSM_CONTROLLER1_NAME, TEST_VALUE_GSM_CELL1_NAME);
        insertIrrelevantDataForThirtyMinsQuery();
        final String actualJSONResultStr = setParamsAndRunQuery(THIRTY_MINUTES);
        // verifyResult(actualJSONResultStr, expectedResult);
    }

    @Test
    public void testThirtyMinuteQueryWithExclusiveTAC() throws URISyntaxException, Exception {
        insertAllLookupData();
        insertEventDataAndGetExpectedResult(TEST_VALUE_GSM_CONTROLLER1_NAME, TEST_VALUE_GSM_CELL1_NAME);
        insertTacIntoExclusiveTacGroup(SAMPLE_TAC);
        final String actualJSONResultStr = setParamsAndRunQuery(THIRTY_MINUTES);
        // verifyResultIsEmpty(actualJSONResultStr);
    }

    private String setParamsAndRunQuery(final String mins) {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TIME_QUERY_PARAM, mins);
        requestParameters.add(TAC, Integer.toString(SAMPLE_TAC));
        requestParameters.add(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID);
        return runQuery(service, requestParameters);
    }

    private void verifyResult(final String json,
            final List<GSMCallFailureTerminalDetailedEventAnalysisResult> expectedResults) throws Exception {
        assertJSONSucceeds(json);
        validateAgainstGridDefinition(json, "NETWORK_DETAILED_EVENT_ANALYSIS_GSM_CALL_FAILURE_TAC");
        final List<GSMCallFailureTerminalDetailedEventAnalysisResult> actualResults = getTranslator().translateResult(
                json, GSMCallFailureTerminalDetailedEventAnalysisResult.class);

        assertEquals(expectedResults, actualResults);
    }

    private void verifyResultIsEmpty(final String json) throws Exception {
        assertJSONSucceeds(json);
        final List<GSMCallFailureTerminalDetailedEventAnalysisResult> actualResults = getTranslator().translateResult(
                json, GSMCallFailureTerminalDetailedEventAnalysisResult.class);

        assertThat(actualResults.size(), is(0));
    }

    private void insertIrrelevantDataForThirtyMinsQuery() throws SQLException {
        // event with irrelevant time
        final String dateTimeNowMinus37Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(37 + GSM_CFA_LATENCY_ON_THIRTY_MIN_QUERY);
        insertRowToRaw(GSM_CALL_DROP_CATEGORY_ID, SAMPLE_TAC, dateTimeNowMinus37Mins);

        // event with irrelevant TAC
        final String dateTimeNowMinus27Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(27 + GSM_CFA_LATENCY_ON_THIRTY_MIN_QUERY);
        insertRowToRaw(GSM_CALL_DROP_CATEGORY_ID, SAMPLE_TAC_2, dateTimeNowMinus27Mins);

        // unused TAC in EXCLUSIVE_TAC group
        insertTacIntoExclusiveTacGroup(SAMPLE_EXCLUSIVE_TAC);
    }

    private List<GSMCallFailureTerminalDetailedEventAnalysisResult> insertEventDataAndGetExpectedResult(
            final String expectedController, final String expectedCell) throws Exception {
        final String dateTimeNowMinus25Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(25 + GSM_CFA_LATENCY_ON_THIRTY_MIN_QUERY);
        final String dateTimeNowMinus26Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(26 + GSM_CFA_LATENCY_ON_THIRTY_MIN_QUERY);
        final String dateTimeNowMinus27Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(27 + GSM_CFA_LATENCY_ON_THIRTY_MIN_QUERY);

        final List<GSMCallFailureTerminalDetailedEventAnalysisResult> expectedResult = new ArrayList<GSMCallFailureTerminalDetailedEventAnalysisResult>();
        expectedResult.add(insertDataToDBAndGetExpectedResult(SAMPLE_TAC, dateTimeNowMinus25Mins, expectedController,
                expectedCell));
        expectedResult.add(insertDataToDBAndGetExpectedResult(SAMPLE_TAC, dateTimeNowMinus26Mins, expectedController,
                expectedCell));
        expectedResult.add(insertDataToDBAndGetExpectedResult(SAMPLE_TAC, dateTimeNowMinus27Mins, expectedController,
                expectedCell));

        return expectedResult;
    }

    private GSMCallFailureTerminalDetailedEventAnalysisResult insertDataToDBAndGetExpectedResult(final int tac,
            final String time, final String expectedController, final String expectedCell) throws SQLException {
        insertRowToRaw(GSM_CALL_DROP_CATEGORY_ID, tac, time);
        return getExpectedResult(time + ".0", TEST_VALUE_IMSI1, tac, GSM_CALL_DROP_CATEGORY_ID_DESC,
                TEST_VALUE_RELEASE_TYPE_DESC, TEST_VALUE_URGENCY_CONDITION_DESC, TEST_VALUE_EXTENDED_DESC,
                expectedController, expectedCell);
    }

    private GSMCallFailureTerminalDetailedEventAnalysisResult getExpectedResult(final String time, final String imsi,
            final int tac, final String eventType, final String releaseTypeDesc, final String causeCode,
            final String extendedCauseCode, final String controller, final String accessArea) {
        final GSMCallFailureTerminalDetailedEventAnalysisResult expectedResult = new GSMCallFailureTerminalDetailedEventAnalysisResult();

        expectedResult.setController(controller);
        expectedResult.setAccessArea(accessArea);
        expectedResult.setImsi(imsi);
        expectedResult.setEventTime(time);
        expectedResult.setEventType(eventType);
        expectedResult.setTac(tac);
        expectedResult.setReleaseType(releaseTypeDesc);
        expectedResult.setExtendedCauseValue(extendedCauseCode);
        return expectedResult;
    }

    private void insertRowToRaw(final String eventID, final int tac, final String time) throws SQLException {
        final Map<String, Object> dataForEventTable = new HashMap<String, Object>();
        dataForEventTable.put(HIER3_ID, TEST_VALUE_HIER3_ID);
        dataForEventTable.put(HIER321_ID, TEST_VALUE_HIER321_ID);
        dataForEventTable.put(TAC, tac);
        dataForEventTable.put(CAUSE_GROUP, TEST_VALUE_CAUSE_GROUP);
        dataForEventTable.put(URGENCY_CONDITION, TEST_VALUE_URGENCY_CONDITION);
        dataForEventTable.put(EXTENDED_CAUSE, TEST_VALUE_EXTENDED_CAUSE);
        dataForEventTable.put(RELEASE_TYPE, TEST_VALUE_RELEASE_TYPE);
        dataForEventTable.put(EVENT_TIME, time);
        dataForEventTable.put(TIMEZONE, "0");
        dataForEventTable.put(IMSI, TEST_VALUE_IMSI1);
        dataForEventTable.put(CATEGORY_ID, eventID);
        dataForEventTable.put(DATETIME_ID, time);
        dataForEventTable.put(RSAI, TEST_RSAI);
        dataForEventTable.put(VAMOS_NEIGHBOR_INDICATOR, TEST_VAMOS_NEIGHBOR_INDICATOR);
        dataForEventTable.put(CHANNEL_TYPE, TEST_CHANNEL_TYPE);
        dataForEventTable.put(MSISDN, TEST_MSISDN);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataForEventTable);

    }

    private void createLookupTables() throws Exception {
        createAndReplaceLookupTable(TEMP_DIM_E_SGEH_TAC, DIM_E_SGEH_TAC, MANUFACTURER, MARKETING_NAME, TAC);
        ;
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_EVENTTYPE, "DIM_E_GSM_CFA_EVENTTYPE", CATEGORY_ID,
                CATEGORY_ID_DESC);
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_URGENCY_CONDITION, "DIM_E_GSM_CFA_URGENCY_CONDITION",
                URGENCY_CONDITION, URGENCY_CONDITION_DESC);
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_RELEASE_TYPE, "DIM_E_GSM_CFA_RELEASE_TYPE", RELEASE_TYPE,
                RELEASE_TYPE_DESC);
        createAndReplaceLookupTable(TEMP_DIM_E_SGEH_HIER321, "DIM_E_SGEH_HIER321", HIERARCHY_1, HIERARCHY_3, VENDOR,
                HIER321_ID, HIER3_ID, RAT);
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_VAMOS_NEIGHBOR_INDICATOR,
                "DIM_E_GSM_CFA_VAMOS_NEIGHBOR_INDICATOR", VAMOS_NEIGHBOR_INDICATOR, VAMOS_PAIR_ALLOCATION_BY_MS);
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_RSAI, "DIM_E_GSM_CFA_RSAI", RSAI, RSAI_DESC);
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_CHANNEL_TYPE, "DIM_E_GSM_CFA_CHANNEL_TYPE", CHANNEL_TYPE,
                CHANNEL_TYPE_DESC);
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_EXTENDED_CAUSE, "DIM_E_GSM_CFA_EXTENDED_CAUSE", EXTENDED_CAUSE,
                EXTENDED_CAUSE_DESC);
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
        insertEventTypeLookupData();
        insertUrgencyConditionLookupData();
        insertExtendedCauseLookupData();
        insertReleaseTypeLookupData();
        insertHashIdLookupData();
        insertCauseGroupLookupData();
        insertVamosNeighborLookupData();
        insertRsaiLookupData();
        insertChannelTypeLookupData();
        insertTacLookupData();
    }

    private void insertEventTypeLookupData() throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(CATEGORY_ID, TEST_VALUE_EVENT_ID);
        valuesForTable.put(CATEGORY_ID_DESC, GSM_CALL_DROP_CATEGORY_ID_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_EVENTTYPE, valuesForTable);
    }

    private void insertUrgencyConditionLookupData() throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(URGENCY_CONDITION, TEST_VALUE_URGENCY_CONDITION);
        valuesForTable.put(URGENCY_CONDITION_DESC, TEST_VALUE_URGENCY_CONDITION_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_URGENCY_CONDITION, valuesForTable);
    }

    private void insertCauseGroupLookupData() throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(CAUSE_GROUP, TEST_VALUE_CAUSE_GROUP);
        valuesForTable.put(CAUSE_GROUP_DESC, TEST_VALUE_CAUSE_GROUP_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_CAUSE_GROUP, valuesForTable);
    }

    private void insertExtendedCauseLookupData() throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(EXTENDED_CAUSE, TEST_VALUE_EXTENDED_CAUSE);
        valuesForTable.put(EXTENDED_CAUSE_DESC, TEST_VALUE_EXTENDED_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_EXTENDED_CAUSE, valuesForTable);
    }

    private void insertReleaseTypeLookupData() throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(RELEASE_TYPE, TEST_VALUE_RELEASE_TYPE);
        valuesForTable.put(RELEASE_TYPE_DESC, TEST_VALUE_RELEASE_TYPE_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_RELEASE_TYPE, valuesForTable);
    }

    private void insertHashIdLookupData() throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(HIERARCHY_3, TEST_VALUE_GSM_CONTROLLER1_NAME);
        valuesForTable.put(HIER321_ID, TEST_VALUE_HIER321_ID);
        valuesForTable.put(HIERARCHY_1, TEST_VALUE_GSM_CELL1_NAME);
        valuesForTable.put(HIER3_ID, TEST_VALUE_HIER3_ID);
        valuesForTable.put(VENDOR, TEST_VALUE_VENDOR);
        valuesForTable.put(RAT, "0");
        insertRow(TEMP_DIM_E_SGEH_HIER321, valuesForTable);
    }

    private void insertVamosNeighborLookupData() throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(VAMOS_NEIGHBOR_INDICATOR, TEST_VAMOS_NEIGHBOR_INDICATOR);
        valuesForTable.put(VAMOS_PAIR_ALLOCATION_BY_MS, TEST_VAMOS_PAIR_ALLOCATION_BY_MS);
        insertRow(TEMP_DIM_E_GSM_CFA_VAMOS_NEIGHBOR_INDICATOR, valuesForTable);

    }

    private void insertRsaiLookupData() throws SQLException {

        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(RSAI, TEST_RSAI);
        valuesForTable.put(RSAI_DESC, TEST_RSAI_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_RSAI, valuesForTable);
    }

    private void insertChannelTypeLookupData() throws SQLException {

        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(CHANNEL_TYPE, TEST_CHANNEL_TYPE);
        valuesForTable.put(CHANNEL_TYPE_DESC, TEST_CHANNEL_TYPE_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_CHANNEL_TYPE, valuesForTable);

    }

    private void insertTacLookupData() throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(MANUFACTURER, TEST_VALUE_MANUFACTURER);
        valuesForTable.put(MARKETING_NAME, TEST_VALUE_MARKETING_NAME);
        valuesForTable.put(TAC, TEST_VALUE_TAC);
        insertRow(TEMP_DIM_E_SGEH_TAC, valuesForTable);
    }

    private void insertTacIntoExclusiveTacGroup(final int tac) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, tac);
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    private void createEventTable() throws Exception {
        final Collection<String> columnsForEventTable = new ArrayList<String>();
        columnsForEventTable.add(HIER3_ID);
        columnsForEventTable.add(HIER321_ID);
        columnsForEventTable.add(TAC);
        columnsForEventTable.add(URGENCY_CONDITION);
        columnsForEventTable.add(EXTENDED_CAUSE);
        columnsForEventTable.add(RELEASE_TYPE);
        columnsForEventTable.add(EVENT_TIME);
        columnsForEventTable.add(TIMEZONE);
        columnsForEventTable.add(IMSI);
        columnsForEventTable.add(CATEGORY_ID);
        columnsForEventTable.add(DATETIME_ID);
        columnsForEventTable.add(RSAI);
        columnsForEventTable.add(VAMOS_NEIGHBOR_INDICATOR);
        columnsForEventTable.add(CHANNEL_TYPE);
        columnsForEventTable.add(MSISDN);
        columnsForEventTable.add(CAUSE_GROUP);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columnsForEventTable);
    }

}
