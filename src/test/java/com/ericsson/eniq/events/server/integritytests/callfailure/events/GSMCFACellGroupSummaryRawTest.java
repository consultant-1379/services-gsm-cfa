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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.AccessAreaSummaryService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.GSMCFAGroupSummaryResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author eatiaro
 * @since 2012
 *
 */
public class GSMCFACellGroupSummaryRawTest extends BaseDataIntegrityTest<GSMCFAGroupSummaryResult> {

    private static final String TEST_VALUE_CATEGORY_ID = GSM_CALL_DROP_CATEGORY_ID;

    private static final String TEST_VALUE_HIER321_ID_1 = "5386564559998864911";

    private static final String TEST_VALUE_HIER321_ID_2 = "5352134559998864911";

    private static final String TEST_VALUE_HIER321_ID_3 = "5352134559953217811";

    private String dateTime;

    private static final String TEST_VALUE_CATEGORY_ID_DESC = "Call Drops";

    private static final String TEST_VALUE_IMSI = "46000608201336";

    private static final String TEST_VALUE_IMSI_1 = "46000936201216";

    private static final int TEST_VALUE_TAC = 100100;

    private static final String TEST_VALUE_VENDOR = "Ericsson";

    private static final String TEST_VALUE_CELL_CONTROL_GROUP = "Control_Group";

    private static final String TEST_VALUE_HIER2 = "";

    private AccessAreaSummaryService gsmCFAAccessAreaEventSummaryService;

    private long hashId;

    @Before
    public void setup() throws Exception {
        gsmCFAAccessAreaEventSummaryService = new AccessAreaSummaryService();
        attachDependencies(gsmCFAAccessAreaEventSummaryService);
        createHashId();
        createEventTable();
        createLookupTables();
        insertLookupData();
        insertDataIntoTacGroupTable();
        insertEventData();
    }

    private void createHashId() {
        hashId = queryUtils.createHashIDForCell("0", TEST_VALUE_GSM_CONTROLLER1_NAME, TEST_VALUE_HIER2,
                TEST_VALUE_GSM_CELL1_NAME, TEST_VALUE_VENDOR);
        System.out.println(hashId);
    }

    @Test
    public void testFiveMinuteQuery() throws URISyntaxException, Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TIME_QUERY_PARAM, FIVE_MINUTES);
        requestParameters.add(TYPE_PARAM, CELL);
        requestParameters.add(GROUP_NAME_PARAM, TEST_VALUE_CELL_GROUP);
        requestParameters.add(NODE_PARAM, TEST_VALUE_GSM_CELL1_NAME + "," + TEST_VALUE_HIER2 + ","
                + TEST_VALUE_GSM_CONTROLLER1_NAME + "," + TEST_VALUE_VENDOR + "," + "0");
        requestParameters.add(CATEGORY_ID, TEST_VALUE_CATEGORY_ID);
        final String result = runQuery(gsmCFAAccessAreaEventSummaryService, requestParameters);
        verifyResult(result);
    }

    private void verifyResult(final String json) throws Exception {
        assertJSONSucceeds(json);
        validateAgainstGridDefinition(json, "NETWORK_GSM_EVENT_ANALYSIS_SUMMARY_CELL_GROUP");
        final List<GSMCFAGroupSummaryResult> results = getTranslator().translateResult(json,
                GSMCFAGroupSummaryResult.class);

        assertThat(results.size(), is(1));
        final GSMCFAGroupSummaryResult result = results.get(0);
        assertThat(result.getCategoryDescription(), is(GSM_CALL_DROP_CATEGORY_ID_DESC));
        assertThat(result.getNumberOfFailures(), is(5));
        assertThat(result.getNumberOfImpactedSubscribers(), is(2));
        assertThat(result.getRatio(), is(50.0));
    }

    private void insertEventData() throws Exception {
        dateTime = DateTimeUtilities.getDateTimeMinus2Minutes();
        insertData(TEST_VALUE_IMSI, TEST_VALUE_CATEGORY_ID, TEST_VALUE_HIER321_ID_1, TEST_VALUE_TAC, dateTime, 3);
        insertData(TEST_VALUE_IMSI, TEST_VALUE_CATEGORY_ID, TEST_VALUE_HIER321_ID_2, TEST_VALUE_TAC, dateTime, 1);
        insertData(TEST_VALUE_IMSI_1, TEST_VALUE_CATEGORY_ID, TEST_VALUE_HIER321_ID_2, TEST_VALUE_TAC, dateTime, 1);
        insertData(TEST_VALUE_IMSI, TEST_VALUE_CATEGORY_ID, TEST_VALUE_HIER321_ID_3, TEST_VALUE_TAC, dateTime, 1);
        insertData(TEST_VALUE_IMSI, TEST_VALUE_CATEGORY_ID, TEST_VALUE_HIER321_ID_1, SAMPLE_EXCLUSIVE_TAC, dateTime, 1);
    }

    private void insertData(final String imsi, final String eventID, final String cellID, final int tac,
            final String time, final int instances) throws SQLException {
        for (int i = 0; i < instances; i++) {
            final Map<String, Object> dataForEventTable = new HashMap<String, Object>();
            dataForEventTable.put(HIER321_ID, cellID);
            dataForEventTable.put(TAC, tac);
            dataForEventTable.put(DATETIME_ID, time);
            dataForEventTable.put(EVENT_TIME, time);
            dataForEventTable.put(TIMEZONE, "0");
            dataForEventTable.put(IMSI, imsi);
            dataForEventTable.put(CATEGORY_ID, eventID);
            insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataForEventTable);
            insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataForEventTable);
        }
    }

    private void createLookupTables() throws Exception {
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_EVENTTYPE, "DIM_E_GSM_CFA_EVENTTYPE", CATEGORY_ID,
                CATEGORY_ID_DESC);
        createAndReplaceLookupTable(TEMP_GROUP_TYPE_E_RAT_VEND_HIER321, "GROUP_TYPE_E_RAT_VEND_HIER321",
                GROUP_NAME_KEY, HIER321_ID);
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
        valuesForTable.put(CATEGORY_ID, TEST_VALUE_CATEGORY_ID);
        valuesForTable.put(CATEGORY_ID_DESC, TEST_VALUE_CATEGORY_ID_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_EVENTTYPE, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(GROUP_NAME_KEY, TEST_VALUE_CELL_GROUP);
        valuesForTable.put(HIER321_ID, TEST_VALUE_HIER321_ID_1);
        insertRow(TEMP_GROUP_TYPE_E_RAT_VEND_HIER321, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(GROUP_NAME_KEY, TEST_VALUE_CELL_GROUP);
        valuesForTable.put(HIER321_ID, TEST_VALUE_HIER321_ID_2);
        insertRow(TEMP_GROUP_TYPE_E_RAT_VEND_HIER321, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(GROUP_NAME_KEY, TEST_VALUE_CELL_CONTROL_GROUP);
        valuesForTable.put(HIER321_ID, TEST_VALUE_HIER321_ID_3);
        insertRow(TEMP_GROUP_TYPE_E_RAT_VEND_HIER321, valuesForTable);
    }

    private void insertDataIntoTacGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    private void createEventTable() throws Exception {
        final Collection<String> columnsForEventTable = new ArrayList<String>();
        columnsForEventTable.add(HIER321_ID);
        columnsForEventTable.add(TAC);
        columnsForEventTable.add(DATETIME_ID);
        columnsForEventTable.add(EVENT_TIME);
        columnsForEventTable.add(TIMEZONE);
        columnsForEventTable.add(IMSI);
        columnsForEventTable.add(CATEGORY_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columnsForEventTable);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_SUC_RAW, columnsForEventTable);
    }

}
