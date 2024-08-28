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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.AccessAreaSubCCDetailedService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.GSMCallFailureCellDetailSCCEventAnalysisResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ewanggu
 * @since 2011
 *
 */

public class GSMCFACellSCCDetailedEventRawTest extends
        BaseDataIntegrityTest<GSMCallFailureCellDetailSCCEventAnalysisResult> {

    private static final long TEST_VALUE_HIER321_ID_1 = 4948639634796658772l;

    private static final long TEST_VALUE_HIER321_ID_2 = 4948639634796658773l;

    private static final long TEST_VALUE_HIER3_ID_1 = 0000000000000000000l;

    private static final long TEST_VALUE_HIER3_ID_2 = 1111111111111111111l;

    private static final String TEST_VALUE_CATEGORY_ID_DESC = "Call Drops";

    private static final String TEST_VALUE_CAUSE_GROUP_1 = "1";

    private static final String TEST_VALUE_CAUSE_GROUP_1_DESC = "Cause1";

    private static final String TEST_VALUE_CAUSE_GROUP_2 = "2";

    private static final String TEST_VALUE_URGENCY_CONDITION = "1";

    private static final String TEST_VALUE_URGENCY_CONDITION_DESC = "Urgency1";

    private static final int TEST_VALUE_TAC = 100100;

    private static final String TEST_VALUE_VENDOR = "Ericsson";

    private static final String TEST_VALUE_HIER2 = "";

    private static final String TEST_IMSI_1 = "11111119";

    private static final String TEST_IMSI_2 = "22222229";

    private static final String TEST_IMSI_3 = "33333339";

    private static final int TEST_VALUE_RELEASE_TYPE = 0;

    private static final String TEST_VALUE_RELEASE_TYPE_DESC = "(MSC) NORMAL RELEASE";

    private static final String TEST_VALUE_EXTENDED_CAUSE = "4";

    private static final String TEST_VAMOS_NEIGHBOR_INDICATOR = "1";

    private static final String TEST_VAMOS_PAIR_ALLOCATION_BY_MS = "Neighbor1";

    private static final String TEST_RSAI = "1";

    private static final String TEST_RSAI_DESC = "RSAI1";

    private static final String TEST_CHANNEL_TYPE = "1";

    private static final String TEST_CHANNEL_TYPE_DESC = "Channel1";

    private static final String TEST_VALUE_CALL_DROP_CATEGORY_ID = "0";

    private static final String TEST_VALUE_CALL_SETUP_CATEGORY_ID = "1";

    private String dateTime;

    private long cellHashId;

    private AccessAreaSubCCDetailedService cellService;

    private final static Map<String, Object> extendedCauseCodesTestMap = new HashMap<String, Object>();

    static {
        extendedCauseCodesTestMap.put("1", "A-INTERFACE, TERRESTRIAL RESOURCE UNAVAILABLE");
        extendedCauseCodesTestMap.put("2", "A-INTERFACE, TERRESTRIAL RESOURCE ALLOCATED");
        extendedCauseCodesTestMap.put("3", "A-INTERFACE, SCCP DISCONNECTION INDICATION");
        extendedCauseCodesTestMap.put("4", "A-INTERFACE, RESET CIRCUIT FROM MSC");
        extendedCauseCodesTestMap.put("5", "A-INTERFACE, TERRESTRIAL RESOURCE FAILURE");
    }

    @Before
    public void setup() throws Exception {
        cellService = new AccessAreaSubCCDetailedService();
        attachDependencies(cellService);
        createHashId();
        createEventTable();
        createLookupTables();
        insertDataIntoTacGroupTable();
        insertAllLookupData();
        insertEventData();
    }

    @Test
    public void testCellFiveMinuteQueryForGrid() throws URISyntaxException, Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TIME_QUERY_PARAM, FIVE_MINUTES);
        requestParameters.add(NODE_PARAM, TEST_VALUE_MSS_CELL_NODE);
        requestParameters.add(DISPLAY_PARAM, GRID_PARAM);
        requestParameters.add(TYPE_PARAM, TYPE_CELL);
        requestParameters.add(CAUSE_CODE_PARAM, TEST_VALUE_CAUSE_GROUP_1);
        requestParameters.add(SUB_CAUSE_CODE_PARAM, TEST_VALUE_EXTENDED_CAUSE);
        requestParameters.add(CATEGORY_ID, TEST_VALUE_CALL_DROP_CATEGORY_ID);
        requestParameters.add(CAUSE_CODE_DESCRIPTION, TEST_VALUE_CAUSE_GROUP_1_DESC);
        requestParameters.add(SUB_CAUSE_CODE_DESCRIPTION, extendedCauseCodesTestMap.get(TEST_VALUE_EXTENDED_CAUSE)
                .toString());
        final String result = runQuery(cellService, requestParameters);
        verifyResultForGrid(result);
    }

    private void verifyResultForGrid(final String json) throws Exception {
        assertJSONSucceeds(json);
        validateAgainstGridDefinition(json, "GSM_NETWORK_CC_SCC_DETAILED_EVENT_ANALYSIS_CELL_DETAIL");
        final List<GSMCallFailureCellDetailSCCEventAnalysisResult> results = getTranslator().translateResult(json,
                GSMCallFailureCellDetailSCCEventAnalysisResult.class);
        assertThat(cellHashId, is(TEST_VALUE_HIER321_ID_1));
        assertThat(results.size(), is(1));

        for (final GSMCallFailureCellDetailSCCEventAnalysisResult result : results) {
            assertThat(result.getExCause(), is(extendedCauseCodesTestMap.get(TEST_VALUE_EXTENDED_CAUSE)));
            assertThat(result.getController(), is(TEST_VALUE_GSM_CONTROLLER1_NAME));
            assertThat(result.getVendor(), is(TEST_VALUE_VENDOR));
            assertThat(result.getEventType(), is(TEST_VALUE_CATEGORY_ID_DESC));
            assertThat(result.getReleaseType(), is(TEST_VALUE_RELEASE_TYPE_DESC));
            assertThat(result.getTac(), is(TEST_VALUE_TAC));
            assertThat(result.getTerminalMake(), is(TEST_VALUE_MANUFACTURER));
            assertThat(result.getTerminalModel(), is(TEST_VALUE_MARKETING_NAME));
            assertThat(result.getImsi(), is(TEST_IMSI_3));
            assertThat(result.getRsai(), is(TEST_RSAI_DESC));
            assertThat(result.getChannel(), is(TEST_CHANNEL_TYPE_DESC));
            assertThat(result.getVamos(), is(TEST_VAMOS_PAIR_ALLOCATION_BY_MS));
            assertThat(result.getUrgency(), is(TEST_VALUE_URGENCY_CONDITION_DESC));
        }
    }

    private void createHashId() {
        cellHashId = queryUtils.createHashIDForCell("GSM", TEST_VALUE_GSM_CONTROLLER1_NAME, TEST_VALUE_HIER2,
                TEST_VALUE_GSM_CELL1_NAME, TEST_VALUE_VENDOR);
    }

    private void insertDataIntoTacGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    private void insertEventData() throws Exception {
        dateTime = DateTimeUtilities.getDateTimeMinus3Minutes();
        insertData(TEST_IMSI_1, TEST_VALUE_TAC, TEST_VALUE_HIER321_ID_1, TEST_VALUE_CAUSE_GROUP_1, "1", dateTime,
                TEST_VALUE_HIER3_ID_1, 1);
        insertData(TEST_IMSI_2, TEST_VALUE_TAC, TEST_VALUE_HIER321_ID_1, TEST_VALUE_CAUSE_GROUP_1, "2", dateTime,
                TEST_VALUE_HIER3_ID_1, 1);
        insertData(TEST_IMSI_1, TEST_VALUE_TAC, TEST_VALUE_HIER321_ID_2, TEST_VALUE_CAUSE_GROUP_1, "3", dateTime,
                TEST_VALUE_HIER3_ID_2, 1);
        insertData(TEST_IMSI_3, TEST_VALUE_TAC, TEST_VALUE_HIER321_ID_1, TEST_VALUE_CAUSE_GROUP_2, "4", dateTime,
                TEST_VALUE_HIER3_ID_1, 1);
        insertData(TEST_IMSI_1, TEST_VALUE_TAC, TEST_VALUE_HIER321_ID_1, TEST_VALUE_CAUSE_GROUP_1, "1", dateTime,
                TEST_VALUE_HIER3_ID_1, 2);
        insertData(TEST_IMSI_2, TEST_VALUE_TAC, TEST_VALUE_HIER321_ID_1, TEST_VALUE_CAUSE_GROUP_1, "2", dateTime,
                TEST_VALUE_HIER3_ID_1, 2);
        insertData(TEST_IMSI_3, TEST_VALUE_TAC, TEST_VALUE_HIER321_ID_1, TEST_VALUE_CAUSE_GROUP_2, "3", dateTime,
                TEST_VALUE_HIER3_ID_1, 3);
        insertData(TEST_IMSI_3, TEST_VALUE_TAC, TEST_VALUE_HIER321_ID_2, TEST_VALUE_CAUSE_GROUP_2, "4", dateTime,
                TEST_VALUE_HIER3_ID_2, 1);
        insertData(TEST_IMSI_3, TEST_VALUE_TAC, TEST_VALUE_HIER321_ID_1, TEST_VALUE_CAUSE_GROUP_1, "4", dateTime,
                TEST_VALUE_HIER3_ID_1, 1);
        insertData(TEST_IMSI_3, SAMPLE_EXCLUSIVE_TAC, TEST_VALUE_HIER321_ID_1, TEST_VALUE_CAUSE_GROUP_1, "4", dateTime,
                TEST_VALUE_HIER3_ID_1, 1);
    }

    private void insertData(final String imsi, final int tac, final long hier321_id, final String causeCode,
            final String extendCause, final String time, final long hier3_id, final int instances) throws SQLException {
        for (int i = 0; i < instances; i++) {
            final Map<String, Object> dataForEventTable = new HashMap<String, Object>();
            dataForEventTable.put(HIER321_ID, hier321_id);
            dataForEventTable.put(TAC, tac);
            dataForEventTable.put(URGENCY_CONDITION, TEST_VALUE_URGENCY_CONDITION);
            dataForEventTable.put(EXTENDED_CAUSE, extendCause);
            dataForEventTable.put(DATETIME_ID, time);
            dataForEventTable.put(EVENT_TIME, time);
            dataForEventTable.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID);
            dataForEventTable.put(RELEASE_TYPE, TEST_VALUE_RELEASE_TYPE);
            dataForEventTable.put(RSAI, TEST_RSAI);
            dataForEventTable.put(VAMOS_NEIGHBOR_INDICATOR, TEST_VAMOS_NEIGHBOR_INDICATOR);
            dataForEventTable.put(CHANNEL_TYPE, TEST_CHANNEL_TYPE);
            dataForEventTable.put(CAUSE_GROUP, causeCode);
            dataForEventTable.put(IMSI, imsi);
            dataForEventTable.put(HIER3_ID, hier3_id);
            insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataForEventTable);
        }
    }

    private void createLookupTables() throws Exception {
        createAndReplaceLookupTable(TEMP_DIM_E_SGEH_TAC, DIM_E_SGEH_TAC, MANUFACTURER, MARKETING_NAME, TAC);
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_EVENTTYPE, "DIM_E_GSM_CFA_EVENTTYPE", CATEGORY_ID,
                CATEGORY_ID_DESC);
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_URGENCY_CONDITION, "DIM_E_GSM_CFA_URGENCY_CONDITION",
                URGENCY_CONDITION, URGENCY_CONDITION_DESC);
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_RELEASE_TYPE, "DIM_E_GSM_CFA_RELEASE_TYPE", RELEASE_TYPE,
                RELEASE_TYPE_DESC);
        createAndReplaceLookupTable(TEMP_DIM_E_SGEH_HIER321, "DIM_E_SGEH_HIER321", HIERARCHY_1, HIERARCHY_3, VENDOR,
                HIER321_ID, RAT);
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_VAMOS_NEIGHBOR_INDICATOR,
                "DIM_E_GSM_CFA_VAMOS_NEIGHBOR_INDICATOR", VAMOS_NEIGHBOR_INDICATOR, VAMOS_PAIR_ALLOCATION_BY_MS);
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_RSAI, "DIM_E_GSM_CFA_RSAI", RSAI, RSAI_DESC);
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_CHANNEL_TYPE, "DIM_E_GSM_CFA_CHANNEL_TYPE", CHANNEL_TYPE,
                CHANNEL_TYPE_DESC);

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

    private void insertAllLookupData() throws SQLException {
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
        valuesForTable.put(HIER321_ID, TEST_VALUE_HIER321_ID_1);
        valuesForTable.put(HIERARCHY_1, TEST_VALUE_GSM_CELL1_NAME);
        valuesForTable.put(VENDOR, TEST_VALUE_VENDOR);
        valuesForTable.put(RAT, "0");
        insertRow(TEMP_DIM_E_SGEH_HIER321, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(HIERARCHY_3, TEST_VALUE_GSM_CONTROLLER1_NAME);
        valuesForTable.put(HIER321_ID, TEST_VALUE_HIER321_ID_2);
        valuesForTable.put(HIERARCHY_1, TEST_VALUE_GSM_CELL1_NAME);
        valuesForTable.put(VENDOR, TEST_VALUE_VENDOR);
        valuesForTable.put(RAT, "0");
        insertRow(TEMP_DIM_E_SGEH_HIER321, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(VAMOS_NEIGHBOR_INDICATOR, TEST_VAMOS_NEIGHBOR_INDICATOR);
        valuesForTable.put(VAMOS_PAIR_ALLOCATION_BY_MS, TEST_VAMOS_PAIR_ALLOCATION_BY_MS);
        insertRow(TEMP_DIM_E_GSM_CFA_VAMOS_NEIGHBOR_INDICATOR, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(RSAI, TEST_RSAI);
        valuesForTable.put(RSAI_DESC, TEST_RSAI_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_RSAI, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(CHANNEL_TYPE, TEST_CHANNEL_TYPE);
        valuesForTable.put(CHANNEL_TYPE_DESC, TEST_CHANNEL_TYPE_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_CHANNEL_TYPE, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(URGENCY_CONDITION, TEST_VALUE_URGENCY_CONDITION);
        valuesForTable.put(URGENCY_CONDITION_DESC, TEST_VALUE_URGENCY_CONDITION_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_URGENCY_CONDITION, valuesForTable);

        valuesForTable.clear();

    }

    private void createEventTable() throws Exception {
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
        columnsForEventTable.add(RSAI);
        columnsForEventTable.add(VAMOS_NEIGHBOR_INDICATOR);
        columnsForEventTable.add(CHANNEL_TYPE);
        columnsForEventTable.add(CAUSE_GROUP);

        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columnsForEventTable);
    }
}
