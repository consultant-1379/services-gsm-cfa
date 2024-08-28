/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.callfailure.events;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
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

import com.ericsson.eniq.events.server.integritytests.stubs.ReplaceTablesWithTempTablesTemplateUtils;
import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.CCSubCCDetailedService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.GSMCallFailureCauseCodeDetailedEventAnalysisResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ewanggu
 * @since 2011
 * 
 */
public class GSMCFACallDropsDetailedEventRawTest extends
        BaseDataIntegrityTest<GSMCallFailureCauseCodeDetailedEventAnalysisResult> {

    private CCSubCCDetailedService service;

    private static final String TEST_VALUE_HIER321_ID = "4948639634796658772";

    private static final String TEST_VALUE_HIER3_ID = "4948639634796658773";

    private static final String TEST_VALUE_URGENCY_CONDITION = "4";

    private static final String TEST_VALUE_URGENCY_CONDITION_EXCLUSIVE = "5";

    private static final String TEST_VALUE_URGENCY_CONDITION_DESC = "LOW SS DOWNLINK";

    private static final String TEST_VALUE_EXTENDED_CAUSE = "4";

    private static final String TEST_VALUE_CATEGORY_ID_DESC = "Call Drops";

    private static final int TEST_VALUE_TAC = 100100;

    private static final String TEST_VALUE_MANUFACTURER = "Mitsubishi";

    private static final String TEST_VALUE_MARKETING_NAME = "G410";

    private static final int TEST_VALUE_RELEASE_TYPE = 0;

    private static final String TEST_VALUE_RELEASE_TYPE_DESC = "(MSC) NORMAL RELEASE";

    // Test Cause Codes    
    private final static Map<String, Object> extendedCauseCodesTestMap = new HashMap<String, Object>();

    static {
        extendedCauseCodesTestMap.put("1", "A-INTERFACE, TERRESTRIAL RESOURCE UNAVAILABLE");
        extendedCauseCodesTestMap.put("2", "A-INTERFACE, TERRESTRIAL RESOURCE ALLOCATED");
        extendedCauseCodesTestMap.put("3", "A-INTERFACE, SCCP DISCONNECTION INDICATION");
        extendedCauseCodesTestMap.put("4", "A-INTERFACE, RESET CIRCUIT FROM MSC");
        extendedCauseCodesTestMap.put("5", "A-INTERFACE, TERRESTRIAL RESOURCE FAILURE");
    }

    @Before
    public void onSetUp() throws Exception {
        service = new CCSubCCDetailedService();
        attachDependencies(service);
        createLookupTables();
        createEventTables();
        insertData();
        insertDataIntoTacGroupTable();
    }

    @Test
    public void testGetDataCauseCodeCallsDropped() throws Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        requestParameters.putSingle(DISPLAY_PARAM, GRID_PARAM);
        requestParameters.putSingle(CAUSE_VALUE, TEST_VALUE_URGENCY_CONDITION);
        requestParameters.putSingle(EXTENDED_CAUSE_VALUE_COLUMN, TEST_VALUE_EXTENDED_CAUSE);
        requestParameters.putSingle(CAUSE_CODE_DESCRIPTION, TEST_VALUE_URGENCY_CONDITION_DESC);
        requestParameters.putSingle(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID);
        requestParameters.putSingle(SUB_CAUSE_CODE_DESCRIPTION, extendedCauseCodesTestMap
                .get(TEST_VALUE_EXTENDED_CAUSE).toString());
        final String json = runQuery(service, requestParameters);
        verifyResult(json);
        validateAgainstGridDefinition(json, "GSM_CFA_CC_CALL_DROPS_EVENT_ANALYSIS");
    }

    private void verifyResult(final String json) throws Exception {
        final List<GSMCallFailureCauseCodeDetailedEventAnalysisResult> result = getTranslator().translateResult(json,
                GSMCallFailureCauseCodeDetailedEventAnalysisResult.class);
        assertThat(result.size(), is(4));
        for (int i = 0; i < 4; i++) {
            final GSMCallFailureCauseCodeDetailedEventAnalysisResult resultForCauseCode1 = result.get(i);
            assertThat(resultForCauseCode1.getReleaseType(), is(TEST_VALUE_RELEASE_TYPE_DESC));
            assertThat(resultForCauseCode1.getTac(), is(TEST_VALUE_TAC));
            assertThat(resultForCauseCode1.getTerminalMake(), is(TEST_VALUE_MANUFACTURER));
            assertThat(resultForCauseCode1.getTerminalModel(), is(TEST_VALUE_MARKETING_NAME));
        }

    }

    private void insertData() throws SQLException {
        insertIntoDimensionTable();
        insertIntoEventTable();
    }

    private void insertDataIntoTacGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    private void insertIntoEventTable() throws SQLException {
        final String timestamp = DateTimeUtilities.getDateTimeMinus2Minutes();
        for (int i = 1; i < 6; i++) {
            final Map<String, Object> valuesForCauseCodeCFATable = new HashMap<String, Object>();
            valuesForCauseCodeCFATable.put(HIER321_ID, TEST_VALUE_HIER321_ID);
            valuesForCauseCodeCFATable.put(URGENCY_CONDITION, TEST_VALUE_URGENCY_CONDITION);
            valuesForCauseCodeCFATable.put(EXTENDED_CAUSE, TEST_VALUE_EXTENDED_CAUSE);
            valuesForCauseCodeCFATable.put(RELEASE_TYPE, TEST_VALUE_RELEASE_TYPE);
            valuesForCauseCodeCFATable.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID);
            valuesForCauseCodeCFATable.put(DATETIME_ID, timestamp);
            valuesForCauseCodeCFATable.put(EVENT_TIME, timestamp);
            valuesForCauseCodeCFATable.put(IMSI, "12345");
            valuesForCauseCodeCFATable.put(HIER3_ID, TEST_VALUE_HIER3_ID);
            if (i == 5) {
                valuesForCauseCodeCFATable.put(TAC, SAMPLE_EXCLUSIVE_TAC);
            } else {
                valuesForCauseCodeCFATable.put(TAC, TEST_VALUE_TAC);
            }
            insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, valuesForCauseCodeCFATable);
        }
        for (int j = 1; j < 6; j++) {
            final Map<String, Object> valuesForCauseCodeCFATable = new HashMap<String, Object>();
            valuesForCauseCodeCFATable.put(HIER321_ID, TEST_VALUE_HIER321_ID);
            valuesForCauseCodeCFATable.put(URGENCY_CONDITION, TEST_VALUE_URGENCY_CONDITION_EXCLUSIVE);
            valuesForCauseCodeCFATable.put(EXTENDED_CAUSE, j);
            valuesForCauseCodeCFATable.put(RELEASE_TYPE, TEST_VALUE_RELEASE_TYPE);
            valuesForCauseCodeCFATable.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID);
            valuesForCauseCodeCFATable.put(DATETIME_ID, timestamp);
            valuesForCauseCodeCFATable.put(EVENT_TIME, timestamp);
            valuesForCauseCodeCFATable.put(IMSI, "12345");
            valuesForCauseCodeCFATable.put(HIER3_ID, TEST_VALUE_HIER3_ID);
            valuesForCauseCodeCFATable.put(TAC, TEST_VALUE_TAC);
            insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, valuesForCauseCodeCFATable);
        }
    }

    private void createLookupTables() throws Exception {
        createAndReplaceLookupTable(TEMP_DIM_E_SGEH_TAC, DIM_E_SGEH_TAC, MANUFACTURER, MARKETING_NAME, TAC);
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_EVENTTYPE, "DIM_E_GSM_CFA_EVENTTYPE", CATEGORY_ID,
                CATEGORY_ID_DESC);
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_RELEASE_TYPE, "DIM_E_GSM_CFA_RELEASE_TYPE", RELEASE_TYPE,
                RELEASE_TYPE_DESC);
        createAndReplaceLookupTable(TEMP_DIM_E_SGEH_HIER321, "DIM_E_SGEH_HIER321", HIERARCHY_1, HIERARCHY_3, HIER3_ID,
                HIER321_ID, RAT);
    }

    private void createAndReplaceLookupTable(final String tempTableName, final String tableNameToReplace,
            final String... columns) throws Exception {
        final Collection<String> columnsForTable = new ArrayList<String>();
        for (final String column : columns) {
            columnsForTable.add(column);
        }
        createTemporaryTable(tempTableName, columnsForTable);
        ReplaceTablesWithTempTablesTemplateUtils.addTableNameToReplace(tableNameToReplace, tempTableName);
    }

    private void insertIntoDimensionTable() throws SQLException {

        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(MANUFACTURER, TEST_VALUE_MANUFACTURER);
        valuesForTable.put(MARKETING_NAME, TEST_VALUE_MARKETING_NAME);
        valuesForTable.put(TAC, TEST_VALUE_TAC);
        insertRow(TEMP_DIM_E_SGEH_TAC, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID);
        valuesForTable.put(CATEGORY_ID_DESC, TEST_VALUE_CATEGORY_ID_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_EVENTTYPE, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(RELEASE_TYPE, TEST_VALUE_RELEASE_TYPE);
        valuesForTable.put(RELEASE_TYPE_DESC, TEST_VALUE_RELEASE_TYPE_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_RELEASE_TYPE, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(HIERARCHY_3, TEST_VALUE_GSM_CONTROLLER1_NAME);
        valuesForTable.put(HIER321_ID, TEST_VALUE_HIER321_ID);
        valuesForTable.put(HIERARCHY_1, TEST_VALUE_GSM_CELL1_NAME);
        valuesForTable.put(HIER3_ID, TEST_VALUE_HIER3_ID);
        valuesForTable.put(RAT, "0");
        insertRow(TEMP_DIM_E_SGEH_HIER321, valuesForTable);
    }

    private void createEventTables() throws Exception {
        final Collection<String> columnsForEventTable = new ArrayList<String>();
        columnsForEventTable.add(URGENCY_CONDITION);
        columnsForEventTable.add(EXTENDED_CAUSE);
        columnsForEventTable.add(CATEGORY_ID);
        columnsForEventTable.add(DATETIME_ID);
        columnsForEventTable.add(IMSI);
        columnsForEventTable.add(TAC);
        columnsForEventTable.add(HIER321_ID);
        columnsForEventTable.add(HIER3_ID);
        columnsForEventTable.add(RELEASE_TYPE);
        columnsForEventTable.add(EVENT_TIME);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columnsForEventTable);

    }

}
