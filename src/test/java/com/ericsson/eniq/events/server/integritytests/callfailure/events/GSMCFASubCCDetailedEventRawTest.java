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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.ControllerSubCCDetailedService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.GSMDetailedEventCFABySubCauseCodeResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author eatiaro
 * @since 2011
 * 
 */

public class GSMCFASubCCDetailedEventRawTest extends BaseDataIntegrityTest<GSMDetailedEventCFABySubCauseCodeResult> {

    private ControllerSubCCDetailedService service;

    private static final String TEST_VALUE_HIER321_ID = "4948639634796658772";

    private static final String TEST_VALUE_HIER3_ID_NO_MATCH = "4948639634796290673";

    private static final String TEST_IMSI_VALUE = "123431234";

    private static final int TEST_IMSI_VALUE_NO_MATCH = 9851247;

    private static final String TEST_VALUE_EXTENDED_CAUSE = "4";

    private static final int TEST_VALUE_TAC = 100100;

    private static final int TEST_VALUE_TAC_NO_MATCH = 321123;

    private static final String TEST_VALUE_MANUFACTURER = "Mitsubishi";

    private static final String TEST_VALUE_MARKETING_NAME = "G410";

    private static final int TEST_VALUE_RELEASE_TYPE = 0;

    private static final String TEST_VALUE_RELEASE_TYPE_DESC = "(MSC) NORMAL RELEASE";

    private static final int NUMBER_OF_MATCHING_EVENTS = 3;

    private static final String TEST_VAMOS_NEIGHBOR_INDICATOR = "1";

    private static final String TEST_VAMOS_PAIR_ALLOCATION_BY_MS = "Neighbor1";

    private static final String TEST_RSAI = "1";

    private static final String TEST_RSAI_DESC = "RSAI1";

    private static final String TEST_CHANNEL_TYPE = "1";

    private static final String TEST_CHANNEL_TYPE_DESC = "Channel1";

    private static final String TEST_VALUE_CAUSE_GROUP_1 = "1";

    private static final String TEST_VALUE_CAUSE_GROUP_1_DESC = "Cause1";

    private static final String TEST_VALUE_CAUSE_GROUP_2 = "2";

    private static final String TEST_URGENCY_CONDITION = "1";

    private static final String TEST_URGENCY_CONDITION_DESC = "Urgency1";

    private static final String TEST_VALUE_CATEGORY_ID_DESC = "Call Drops";

    private static final String TEST_VALUE_EXTENDED_CAUSE_NO_MATCH = "1";

    @SuppressWarnings("serial")
    private final static Map<String, String> extendedCauseCodesTestMap = new HashMap<String, String>() {
        {
            put("1", "A-INTERFACE, TERRESTRIAL RESOURCE UNAVAILABLE");
            put("2", "A-INTERFACE, TERRESTRIAL RESOURCE ALLOCATED");
            put("3", "A-INTERFACE, SCCP DISCONNECTION INDICATION");
            put("4", "A-INTERFACE, RESET CIRCUIT FROM MSC");
            put("5", "A-INTERFACE, TERRESTRIAL RESOURCE FAILURE");
        }
    };

    private long getHashIdForBSC1(final String node) {
        final String[] nodeComponents = node.split(DELIMITER);
        return queryUtils.createHashIDForController(nodeComponents[2], nodeComponents[0], nodeComponents[1]);
    }

    @Before
    public void onSetUp() throws Exception {
        service = new ControllerSubCCDetailedService();
        attachDependencies(service);
        createLookupTables();
        createEventTables();
        insertData();
        //insertTacGroupData();
    }

    @Test
    public void testRawDataFiveMinutesQuery() throws Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        requestParameters.putSingle(DISPLAY_PARAM, GRID_PARAM);
        requestParameters.putSingle(TYPE_PARAM, TYPE_BSC);
        requestParameters.putSingle(CAUSE_CODE_PARAM, TEST_VALUE_CAUSE_GROUP_1);
        requestParameters.putSingle(SUB_CAUSE_CODE_PARAM, TEST_VALUE_EXTENDED_CAUSE);
        requestParameters.putSingle(NODE_PARAM, TEST_VALUE_MSS_CONTROLLER_NODE);
        requestParameters.putSingle(CAUSE_CODE_DESCRIPTION, TEST_VALUE_CAUSE_GROUP_1_DESC);
        requestParameters.putSingle(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID);
        requestParameters.putSingle(SUB_CAUSE_CODE_DESCRIPTION, extendedCauseCodesTestMap
                .get(TEST_VALUE_EXTENDED_CAUSE).toString());
        final String jsonResult = runQuery(service, requestParameters);
        verifyResult(jsonResult);
        validateAgainstGridDefinition(jsonResult, "GSM_NETWORK_BSC_CC_SCC_DETAILED_EVENT_ANALYSIS");
    }

    private void verifyResult(final String json) throws Exception {
        final List<GSMDetailedEventCFABySubCauseCodeResult> resultRecord = getTranslator().translateResult(json,
                GSMDetailedEventCFABySubCauseCodeResult.class);
        assertThat(resultRecord.size(), is(NUMBER_OF_MATCHING_EVENTS));
        for (int i = 0; i < NUMBER_OF_MATCHING_EVENTS; i++) {
            final GSMDetailedEventCFABySubCauseCodeResult result = resultRecord.get(i);

            assertThat(result.getExCause(), is(extendedCauseCodesTestMap.get(TEST_VALUE_EXTENDED_CAUSE)));
            assertThat(result.getController(), is(TEST_VALUE_GSM_CONTROLLER1_NAME));
            assertThat(result.getVendor(), is(ERICSSON));
            assertThat(result.getEventType(), is(TEST_VALUE_CATEGORY_ID_DESC));
            assertThat(result.getReleaseType(), is(TEST_VALUE_RELEASE_TYPE_DESC));
            assertThat(result.getTac(), is(TEST_VALUE_TAC));
            assertThat(result.getTerminalMake(), is(TEST_VALUE_MANUFACTURER));
            assertThat(result.getTerminalModel(), is(TEST_VALUE_MARKETING_NAME));
            assertThat(result.getImsi(), is(TEST_IMSI_VALUE));
            assertThat(result.getRsai(), is(TEST_RSAI_DESC));
            assertThat(result.getChannel(), is(TEST_CHANNEL_TYPE_DESC));
            assertThat(result.getVamos(), is(TEST_VAMOS_PAIR_ALLOCATION_BY_MS));
            assertThat(result.getUrgency(), is(TEST_URGENCY_CONDITION_DESC));
        }

    }

    private void insertData() throws SQLException {
        insertLookupData();
        insertRawEventData();
    }

    private void insertRawEventData() throws SQLException {
        final String timestamp = DateTimeUtilities.getDateTimeMinus3Minutes();
        final Map<String, Object> valuesForCauseCodeCFATable = new HashMap<String, Object>();

        //Insert matching data
        for (int i = 0; i < NUMBER_OF_MATCHING_EVENTS; i++) {
            valuesForCauseCodeCFATable.put(HIER3_ID, getHashIdForBSC1(TEST_VALUE_MSS_CONTROLLER_NODE));
            valuesForCauseCodeCFATable.put(URGENCY_CONDITION, TEST_URGENCY_CONDITION);
            valuesForCauseCodeCFATable.put(EXTENDED_CAUSE, TEST_VALUE_EXTENDED_CAUSE);
            valuesForCauseCodeCFATable.put(HIER321_ID, TEST_VALUE_HIER321_ID);
            valuesForCauseCodeCFATable.put(RELEASE_TYPE, TEST_VALUE_RELEASE_TYPE);
            valuesForCauseCodeCFATable.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID);
            valuesForCauseCodeCFATable.put(DATETIME_ID, timestamp);
            valuesForCauseCodeCFATable.put(IMSI, TEST_IMSI_VALUE);
            valuesForCauseCodeCFATable.put(EVENT_TIME, timestamp);
            valuesForCauseCodeCFATable.put(TAC, TEST_VALUE_TAC);
            valuesForCauseCodeCFATable.put(RELEASE_TYPE, TEST_VALUE_RELEASE_TYPE);
            valuesForCauseCodeCFATable.put(RSAI, TEST_RSAI);
            valuesForCauseCodeCFATable.put(VAMOS_NEIGHBOR_INDICATOR, TEST_VAMOS_NEIGHBOR_INDICATOR);
            valuesForCauseCodeCFATable.put(CHANNEL_TYPE, TEST_CHANNEL_TYPE);
            valuesForCauseCodeCFATable.put(CAUSE_GROUP, TEST_VALUE_CAUSE_GROUP_1);

            insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, valuesForCauseCodeCFATable);
        }

        //insert non-matching data

        //Controller and IMSI do not match
        valuesForCauseCodeCFATable.remove(HIER3_ID);
        valuesForCauseCodeCFATable.put(HIER3_ID, TEST_VALUE_HIER3_ID_NO_MATCH);
        valuesForCauseCodeCFATable.remove(IMSI);
        valuesForCauseCodeCFATable.put(IMSI, TEST_IMSI_VALUE_NO_MATCH);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, valuesForCauseCodeCFATable);

        //Cause code(urgency condition) and TAC do not match
        valuesForCauseCodeCFATable.remove(CAUSE_GROUP);
        valuesForCauseCodeCFATable.put(CAUSE_GROUP, TEST_VALUE_CAUSE_GROUP_2);
        valuesForCauseCodeCFATable.remove(TAC);
        valuesForCauseCodeCFATable.put(TAC, TEST_VALUE_TAC_NO_MATCH);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, valuesForCauseCodeCFATable);

        //Sub cause code (extended cause) and IMSI do not match
        valuesForCauseCodeCFATable.remove(EXTENDED_CAUSE);
        valuesForCauseCodeCFATable.put(EXTENDED_CAUSE, TEST_VALUE_EXTENDED_CAUSE_NO_MATCH);
        valuesForCauseCodeCFATable.remove(IMSI);
        valuesForCauseCodeCFATable.put(IMSI, TEST_IMSI_VALUE_NO_MATCH);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, valuesForCauseCodeCFATable);

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
                HIER321_ID, HIER3_ID, RAT);
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

    private void insertLookupData() throws SQLException {
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
        valuesForTable.put(URGENCY_CONDITION, TEST_URGENCY_CONDITION);
        valuesForTable.put(URGENCY_CONDITION_DESC, TEST_URGENCY_CONDITION_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_URGENCY_CONDITION, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(HIERARCHY_3, TEST_VALUE_GSM_CONTROLLER1_NAME);
        valuesForTable.put(HIER321_ID, TEST_VALUE_HIER321_ID);
        valuesForTable.put(HIERARCHY_1, TEST_VALUE_GSM_CELL1_NAME);
        valuesForTable.put(HIER3_ID, getHashIdForBSC1(TEST_VALUE_MSS_CONTROLLER_NODE));
        valuesForTable.put(RAT, RAT_FOR_GSM);
        valuesForTable.put(VENDOR, ERICSSON);
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
        columnsForEventTable.add(RSAI);
        columnsForEventTable.add(VAMOS_NEIGHBOR_INDICATOR);
        columnsForEventTable.add(CHANNEL_TYPE);
        columnsForEventTable.add(CAUSE_GROUP);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columnsForEventTable);
    }
}
