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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.ControllerSummaryService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.GSMBscCallFailureEventSummaryResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class GSMCFABscEventSummaryRawTest extends BaseDataIntegrityTest<GSMBscCallFailureEventSummaryResult> {

    private ControllerSummaryService gsmCFABscEventSummaryService;

    @Before
    public void onSetUp() throws Exception {
        gsmCFABscEventSummaryService = new ControllerSummaryService();
        attachDependencies(gsmCFABscEventSummaryService);
        createTables();
        insertDataIntoTacGroupTable();
        insertLookupData();
        insertSuccessData();
    }

    @Test
    public void testFiveMinuteQuery() throws URISyntaxException, Exception {
        insertEventData(DateTimeUtilities.getDateTimeMinus2Minutes());
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        requestParameters.add(TIME_QUERY_PARAM, FIVE_MINUTES);
        requestParameters.add(CONTROLLER_SQL_ID, Long.toString(TEST_VALUE_GSM_HIER3_ID_BSC1));
        final String result = runQuery(gsmCFABscEventSummaryService, requestParameters);
        verifyResult(result);
    }

    private void verifyResult(final String json) throws Exception {
        assertJSONSucceeds(json);
        final List<GSMBscCallFailureEventSummaryResult> results = getTranslator().translateResult(json,
                GSMBscCallFailureEventSummaryResult.class);

        assertThat(results.size(), is(2));
        validateAgainstGridDefinition(json, "NETWORK_GSM_EVENT_ANALYSIS_SUMMARY_BSC");
        GSMBscCallFailureEventSummaryResult result = results.get(0);

        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(BSC1));
        assertThat(result.getCategoryDescription(), is(GSM_CALL_SETUP_FAILURES_CATEGORY_ID_DESC));
        assertThat(result.getNumberOfFailures(), is(6));
        assertThat(result.getNumberOfSubscribers(), is(3));
        assertThat(result.getNumberOfImpactedCells(), is(3));
        assertThat(result.getFailureRatio(), is(27.27));

        //Verifying result for call drop
        result = results.get(1);

        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(BSC1));
        assertThat(result.getCategoryDescription(), is(GSM_CALL_DROP_CATEGORY_ID_DESC));
        assertThat(result.getNumberOfFailures(), is(6));
        assertThat(result.getNumberOfSubscribers(), is(3));
        assertThat(result.getNumberOfImpactedCells(), is(3));
        assertThat(result.getFailureRatio(), is(27.27));
    }

    private void insertDataIntoTacGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    private void insertEventData(final String dateTime) throws Exception {
        insertData(TEST_VALUE_GSM_HIER3_ID_BSC1, TEST_VALUE_HIER321_ID_1, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER,
                SAMPLE_TAC, dateTime, TEST_VALUE_IMSI_1, 4);
        insertData(TEST_VALUE_GSM_HIER3_ID_BSC1, TEST_VALUE_HIER321_ID_2, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER,
                SAMPLE_TAC, dateTime, TEST_VALUE_IMSI_2, 1);
        insertData(TEST_VALUE_GSM_HIER3_ID_BSC1, TEST_VALUE_HIER321_ID_3, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER,
                SAMPLE_TAC, dateTime, TEST_VALUE_IMSI_3, 1);

        insertData(TEST_VALUE_GSM_HIER3_ID_BSC1, TEST_VALUE_HIER321_ID_1,
                GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER, SAMPLE_TAC, dateTime, TEST_VALUE_IMSI_1, 4);
        insertData(TEST_VALUE_GSM_HIER3_ID_BSC1, TEST_VALUE_HIER321_ID_2,
                GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER, SAMPLE_TAC, dateTime, TEST_VALUE_IMSI_2, 1);
        insertData(TEST_VALUE_GSM_HIER3_ID_BSC1, TEST_VALUE_HIER321_ID_3,
                GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER, SAMPLE_TAC, dateTime, TEST_VALUE_IMSI_3, 1);

        //Valid entry for different BSC
        insertData(TEST_VALUE_GSM_HIER3_ID_BSC2, TEST_VALUE_HIER321_ID_4, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER,
                SAMPLE_TAC, dateTime, TEST_VALUE_IMSI_1, 1);

        //Irrelevant data in terms of time range
        insertData(TEST_VALUE_GSM_HIER3_ID_BSC1, TEST_VALUE_HIER321_ID_1, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER,
                SAMPLE_TAC, DateTimeUtilities.getDateTimeMinus48Hours(), TEST_VALUE_IMSI_1, 1);
        //Exclusive TAC
        insertData(TEST_VALUE_GSM_HIER3_ID_BSC1, TEST_VALUE_HIER321_ID_1, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER,
                SAMPLE_EXCLUSIVE_TAC, dateTime, TEST_VALUE_IMSI_1, 1);

    }

    private void insertData(final long hier3_id, final long hier321_id, final int categoryId, final int tac,
            final String time, final long imsi, final int instances) throws SQLException {
        for (int i = 0; i < instances; i++) {
            final Map<String, Object> dataForEventTable = new HashMap<String, Object>();
            dataForEventTable.put(HIER3_ID, hier3_id);
            dataForEventTable.put(HIER321_ID, hier321_id);
            dataForEventTable.put(TAC, tac);
            dataForEventTable.put(IMSI, imsi);
            dataForEventTable.put(CATEGORY_ID, categoryId);
            dataForEventTable.put(DATETIME_ID, time);
            insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataForEventTable);
        }
    }

    private void insertLookupData() throws Exception {
        final Map<String, Object> tableData = new HashMap<String, Object>();
        tableData.put(HIERARCHY_3, BSC1);
        tableData.put(HIER3_ID, TEST_VALUE_GSM_HIER3_ID_BSC1);
        tableData.put(VENDOR, ERICSSON);
        tableData.put(RAT, RAT_INTEGER_VALUE_FOR_2G);
        insertRow(TEMP_DIM_E_SGEH_HIER321, tableData);

        tableData.clear();
        tableData.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        tableData.put(CATEGORY_ID_DESC, GSM_CALL_DROP_CATEGORY_ID_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_EVENTTYPE, tableData);

        tableData.clear();
        tableData.put(CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER);
        tableData.put(CATEGORY_ID_DESC, GSM_CALL_SETUP_FAILURES_CATEGORY_ID_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_EVENTTYPE, tableData);

    }

    private void insertSuccessData() throws Exception {
        final Map<String, Object> successData = new HashMap<String, Object>();
        //insert 10 successful calls for this BSC
        for (int i = 0; i < 10; ++i) {
            successData.put(HIER3_ID, TEST_VALUE_GSM_HIER3_ID_BSC1);
            successData.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinus2Minutes());
            successData.put(TAC, SAMPLE_TAC);
            insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, successData);
        }

        //put some values outside of time range
        successData.clear();
        for (int i = 0; i < 3; ++i) {
            successData.put(HIER3_ID, TEST_VALUE_GSM_HIER3_ID_BSC1);
            successData.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinus30Minutes());
            successData.put(TAC, SAMPLE_TAC);
            insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, successData);
        }

        //put some successful calls for a different BSC
        successData.clear();
        for (int i = 0; i < 15; ++i) {
            successData.put(HIER3_ID, TEST_VALUE_GSM_HIER3_ID_BSC2);
            successData.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinus2Minutes());
            successData.put(TAC, SAMPLE_TAC);
            insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, successData);
        }
    }

    private void createTables() throws Exception {
        final Collection<String> columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE = new ArrayList<String>();
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.add(CATEGORY_ID_DESC);
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.add(CATEGORY_ID);
        createTemporaryTable(TEMP_DIM_E_GSM_CFA_EVENTTYPE, columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE);

        final Collection<String> columnsFor_DIM_E_SGEH_HIER321 = new ArrayList<String>();
        columnsFor_DIM_E_SGEH_HIER321.add(HIER3_ID);
        columnsFor_DIM_E_SGEH_HIER321.add(HIERARCHY_3);
        columnsFor_DIM_E_SGEH_HIER321.add(VENDOR);
        columnsFor_DIM_E_SGEH_HIER321.add(RAT);
        createTemporaryTable(TEMP_DIM_E_SGEH_HIER321, columnsFor_DIM_E_SGEH_HIER321);

        final Collection<String> columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW = new ArrayList<String>();
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(HIER3_ID);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(HIER321_ID);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(IMSI);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(TAC);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(DATETIME_ID);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(CATEGORY_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);

        final Collection<String> columnsFor_TEMP_EVENT_E_GSM_CFA_SUC_RAW = new ArrayList<String>();
        columnsFor_TEMP_EVENT_E_GSM_CFA_SUC_RAW.add(HIER3_ID);
        columnsFor_TEMP_EVENT_E_GSM_CFA_SUC_RAW.add(DATETIME_ID);
        columnsFor_TEMP_EVENT_E_GSM_CFA_SUC_RAW.add(TAC);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_SUC_RAW, columnsFor_TEMP_EVENT_E_GSM_CFA_SUC_RAW);

        ReplaceTablesWithTempTablesTemplateUtils.addTableNameToReplace("DIM_E_SGEH_HIER321", TEMP_DIM_E_SGEH_HIER321);
        ReplaceTablesWithTempTablesTemplateUtils.addTableNameToReplace("DIM_E_GSM_CFA_EVENTTYPE",
                TEMP_DIM_E_GSM_CFA_EVENTTYPE);
    }

    private static final long TEST_VALUE_HIER321_ID_1 = 1111111111L;

    private static final long TEST_VALUE_HIER321_ID_2 = 2222222222L;

    private static final long TEST_VALUE_HIER321_ID_3 = 3333333333L;

    private static final long TEST_VALUE_HIER321_ID_4 = 4444444444L;

    private static final long TEST_VALUE_IMSI_1 = 9812326598L;

    private static final long TEST_VALUE_IMSI_2 = 1212326512L;

    private static final long TEST_VALUE_IMSI_3 = 3432654334L;

    public static final String TEMP_EVENT_E_GSM_CFA_SUC_RAW = "#EVENT_E_GSM_CFA_SUC_RAW";
}
