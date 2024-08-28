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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.CCSubCCSummaryService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.GSMCallFailureSubCauseCodeEventSummaryResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ewanggu
 * @since 2012
 * 
 */
public class GSMCFASCCEventSummaryRawTest extends BaseDataIntegrityTest<GSMCallFailureSubCauseCodeEventSummaryResult> {

    private CCSubCCSummaryService service;

    private static final String TEMP_DIM_E_GSM_CFA_EXTENDED_CAUSE = "#DIM_E_GSM_CFA_EXTENDED_CAUSE";

    private static final String TEST_VALUE_URGENCY_CONDITION = "4";

    private static final String TEST_VALUE_URGENCY_CONDITION_DESC = "LOW SS DOWNLINK";

    private static final int TEST_VALUE_TAC = 100100;

    // Test Extended Cause Codes    
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
        service = new CCSubCCSummaryService();
        attachDependencies(service);
        createLookupTables();
        createEventTables();
        insertData();
        insertDataIntoTacGroupTable();
    }

    @Test
    public void testGetDataSubCauseCodeCallsDropped() throws Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        requestParameters.putSingle(DISPLAY_PARAM, GRID_PARAM);
        requestParameters.putSingle(CAUSE_CODE_ID, TEST_VALUE_URGENCY_CONDITION);
        requestParameters.putSingle(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID);
        requestParameters.putSingle(CAUSE_CODE_DESCRIPTION, TEST_VALUE_URGENCY_CONDITION_DESC);
        final String json = runQuery(service, requestParameters);
        verifyResult(json);
        validateAgainstGridDefinition(json, "GSM_CFA_SCC_SUMMARY_EVENT_ANALYSIS");
    }

    private void verifyResult(final String json) throws Exception {
        final List<GSMCallFailureSubCauseCodeEventSummaryResult> result = getTranslator().translateResult(json,
                GSMCallFailureSubCauseCodeEventSummaryResult.class);
        assertThat(result.size(), is(4));
        for (int i = 0; i < 4; i++) {
            final GSMCallFailureSubCauseCodeEventSummaryResult resultForCauseCode1 = result.get(i);
            assertThat(resultForCauseCode1.getCauseCode(), is(Integer.parseInt(TEST_VALUE_URGENCY_CONDITION)));
            assertThat(resultForCauseCode1.getCauseCodeDescription(), is(TEST_VALUE_URGENCY_CONDITION_DESC));
            assertThat(resultForCauseCode1.getExtendedCauseCodeId(), is(4 - i));
            assertThat(resultForCauseCode1.getExtendedCauseCodeDescription(),
                    is(extendedCauseCodesTestMap.get((4 - i) + "")));
            assertThat(resultForCauseCode1.getFailures(), is(4 - i));
            assertThat(resultForCauseCode1.getImpactedSubscriber(), is(4 - i));

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
            for (int j = 1; j <= i; j++) {
                final Map<String, Object> valuesForCauseCodeCFATable = new HashMap<String, Object>();
                valuesForCauseCodeCFATable.put(URGENCY_CONDITION, TEST_VALUE_URGENCY_CONDITION);
                valuesForCauseCodeCFATable.put(EXTENDED_CAUSE, i);
                valuesForCauseCodeCFATable.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID);
                valuesForCauseCodeCFATable.put(DATETIME_ID, timestamp);
                valuesForCauseCodeCFATable.put(IMSI, j);
                if (i == 5) {
                    valuesForCauseCodeCFATable.put(TAC, SAMPLE_EXCLUSIVE_TAC);
                } else {
                    valuesForCauseCodeCFATable.put(TAC, TEST_VALUE_TAC);
                }
                insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, valuesForCauseCodeCFATable);
            }
        }
    }

    private void createLookupTables() throws Exception {
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_EXTENDED_CAUSE, "DIM_E_GSM_CFA_EXTENDED_CAUSE", EXTENDED_CAUSE,
                EXTENDED_CAUSE_DESC);
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
        for (int i = 1; i < 6; i++) {
            final Map<String, Object> values = new HashMap<String, Object>();
            values.put(EXTENDED_CAUSE, i);
            values.put(EXTENDED_CAUSE_DESC, extendedCauseCodesTestMap.get(Integer.toString(i)));
            insertRow(TEMP_DIM_E_GSM_CFA_EXTENDED_CAUSE, values);
        }
    }

    private void createEventTables() throws Exception {
        final Collection<String> columnsForEventTable = new ArrayList<String>();
        columnsForEventTable.add(URGENCY_CONDITION);
        columnsForEventTable.add(EXTENDED_CAUSE);
        columnsForEventTable.add(CATEGORY_ID);
        columnsForEventTable.add(DATETIME_ID);
        columnsForEventTable.add(IMSI);
        columnsForEventTable.add(TAC);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columnsForEventTable);

    }
}
