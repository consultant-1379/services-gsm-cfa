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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.ControllerSummaryService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.GSMBscCallFailureEventSummaryResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class GSMCFABscEventSummaryAggTest extends BaseDataIntegrityTest<GSMBscCallFailureEventSummaryResult> {

    private static final String TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_ERR_15MIN = "#EVENT_E_GSM_CFA_HIER321_EVENTID_ERR_15MIN";

    private static final String TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_SUC_15MIN = "#EVENT_E_GSM_CFA_HIER321_EVENTID_SUC_15MIN";

    private ControllerSummaryService gsmCFABscEventSummaryService;

    private static final int IMSI_TEST_VALUE_1 = 1;

    private static final int IMSI_TEST_VALUE_2 = 2;

    private static final int IMSI_TEST_VALUE_3 = 3;

    private static final int CATEGORY_ID_NO_MATCH = 5;

    private static final String BSCA01_NODE = "BSCA01,Ericsson,GSM";

    private static final long BSCA01_HIER3_ID = 8041237897185549474L;

    /**
     * 1. Create tables. 2. Insert test data to the tables.
     * 
     * @throws Exception
     */
    @Before
    public void onSetUp() throws Exception {
        gsmCFABscEventSummaryService = new ControllerSummaryService();
        attachDependencies(gsmCFABscEventSummaryService);
        createTables();
        insertDataIntoTacGroupTable();
        insertData();
    }

    @Test
    public void testGetEventSummaryData() throws Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        requestParameters.add(TIME_QUERY_PARAM, ONE_DAY);
        requestParameters.add(NODE_PARAM, BSCA01_NODE);
        requestParameters.add(TYPE_PARAM, TYPE_BSC);
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
        assertThat(result.getNumberOfFailures(), is(3));
        assertThat(result.getNumberOfImpactedCells(), is(3));
        assertThat(result.getNumberOfSubscribers(), is(1));
        assertThat(result.getFailureRatio(), is(18.75));

        result = results.get(1);

        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(BSC1));
        assertThat(result.getCategoryDescription(), is(GSM_CALL_DROP_CATEGORY_ID_DESC));
        assertThat(result.getNumberOfFailures(), is(3));
        assertThat(result.getNumberOfImpactedCells(), is(3));
        assertThat(result.getNumberOfSubscribers(), is(2));
        assertThat(result.getFailureRatio(), is(18.75));
    }

    private void createTables() throws Exception {
        final Collection<String> columnsForEventTable = new ArrayList<String>();
        columnsForEventTable.add(HIER3_ID);
        columnsForEventTable.add(HIER321_ID);
        columnsForEventTable.add(NO_OF_ERRORS);
        columnsForEventTable.add(CATEGORY_ID);
        columnsForEventTable.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_ERR_15MIN, columnsForEventTable);

        columnsForEventTable.clear();
        columnsForEventTable.add(HIER3_ID);
        columnsForEventTable.add(DATETIME_ID);
        columnsForEventTable.add(NO_OF_SUCCESSES);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_SUC_15MIN, columnsForEventTable);

        final Collection<String> columnsFor_DIM_E_SGEH_HIER3 = new ArrayList<String>();
        columnsFor_DIM_E_SGEH_HIER3.add(HIERARCHY_3);
        columnsFor_DIM_E_SGEH_HIER3.add(HIER3_ID);
        columnsFor_DIM_E_SGEH_HIER3.add(VENDOR);
        columnsFor_DIM_E_SGEH_HIER3.add(RAT);
        createTemporaryTable(TEMP_DIM_E_SGEH_HIER321, columnsFor_DIM_E_SGEH_HIER3);

        final Collection<String> columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE = new ArrayList<String>();
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.add(CATEGORY_ID_DESC);
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.add(CATEGORY_ID);
        createTemporaryTable(TEMP_DIM_E_GSM_CFA_EVENTTYPE, columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE);

        final Collection<String> columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW = new ArrayList<String>();
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(HIER3_ID);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(HIER321_ID);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(IMSI);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(TAC);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(DATETIME_ID);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(CATEGORY_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);

        ReplaceTablesWithTempTablesTemplateUtils.addTableNameToReplace("DIM_E_SGEH_HIER321", TEMP_DIM_E_SGEH_HIER321);
        ReplaceTablesWithTempTablesTemplateUtils.addTableNameToReplace("DIM_E_GSM_CFA_EVENTTYPE",
                TEMP_DIM_E_GSM_CFA_EVENTTYPE);
        ReplaceTablesWithTempTablesTemplateUtils.addTableNameToReplace("EVENT_E_GSM_CFA_HIER321_EVENTID_ERR_15MIN",
                TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_ERR_15MIN);
        ReplaceTablesWithTempTablesTemplateUtils.addTableNameToReplace("EVENT_E_GSM_CFA_HIER321_EVENTID_SUC_15MIN",
                TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_SUC_15MIN);
        ReplaceTablesWithTempTablesTemplateUtils.addTableNameToReplace("EVENT_E_GSM_CFA_ERR_RAW",
                TEMP_EVENT_E_GSM_CFA_ERR_RAW);
    }

    private void populateHier321Table() throws SQLException {
        insertRowIntoHier321Table(RAT_FOR_GSM, BSC1, ERICSSON, BSCA01_HIER3_ID);
    }

    private void insertRowIntoHier321Table(final int rat, final String controller, final String vendor,
            final long hier3Id) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(RAT, rat);
        valuesForTable.put(HIERARCHY_3, controller);
        valuesForTable.put(VENDOR_PARAM_UPPER_CASE, vendor);
        valuesForTable.put(HIER3_ID, Long.toString(hier3Id));
        insertRow(TEMP_DIM_E_SGEH_HIER321, valuesForTable);
    }

    private void insertData() throws Exception {

        populateHier321Table();

        insertIntoRawTable();

        insertEventTypeTable();

        insertSuccessData();

        final String dateTime = DateTimeUtilities.getDateTimeMinusHours(1);

        insertRowsIntoAggregationView(BSCA01_HIER3_ID, TEST_VALUE_GSM_HIER321_ID_CELL1, dateTime, 1);
        insertRowsIntoAggregationView(BSCA01_HIER3_ID, TEST_VALUE_GSM_HIER321_ID_CELL2, dateTime, 1);
        insertRowsIntoAggregationView(BSCA01_HIER3_ID, TEST_VALUE_GSM_HIER321_ID_CELL3, dateTime, 1);
        insertRowsIntoAggregationView(TEST_VALUE_GSM_HIER3_ID_BSC2, TEST_VALUE_GSM_HIER321_ID_CELL1, dateTime, 1);

    }

    private void insertIntoRawTable() throws Exception {
        final Map<String, Object> rawData = new HashMap<String, Object>();
        rawData.put(HIER3_ID, BSCA01_HIER3_ID);
        rawData.put(HIER321_ID, TEST_VALUE_GSM_HIER321_ID_CELL1);
        rawData.put(IMSI, IMSI_TEST_VALUE_1);
        rawData.put(TAC, SAMPLE_TAC);
        rawData.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusHours(1));
        rawData.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);

        for (int i = 0; i < 3; ++i) {
            insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, rawData);
        }

        //Insert a record with exclusive TAC
        rawData.remove(TAC);
        rawData.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, rawData);

        //Make sure that the event with a different category id will not be counted
        rawData.remove(CATEGORY_ID);
        rawData.put(CATEGORY_ID, CATEGORY_ID_NO_MATCH);
        //Replace the correct TAC
        rawData.remove(TAC);
        rawData.put(TAC, SAMPLE_TAC);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, rawData);
        //Replace the correct Category_id
        rawData.remove(CATEGORY_ID);
        rawData.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);

        //Insert a record for a different subscriber (tweak IMSI)
        rawData.remove(IMSI);
        rawData.put(IMSI, IMSI_TEST_VALUE_2);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, rawData);

        //IMSI that is out of range timewise (tweak datetime ID and IMSI)
        rawData.remove(DATETIME_ID);
        rawData.put(DATETIME_ID, DateTimeUtilities.getDateMinus36Hours());
        rawData.remove(IMSI);
        rawData.put(IMSI, IMSI_TEST_VALUE_3);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, rawData);

        rawData.clear();
        rawData.put(HIER3_ID, BSCA01_HIER3_ID);
        rawData.put(HIER321_ID, TEST_VALUE_GSM_HIER321_ID_CELL1);
        rawData.put(IMSI, IMSI_TEST_VALUE_1);
        rawData.put(TAC, SAMPLE_TAC);
        rawData.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusHours(1));
        rawData.put(CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER);

        for (int i = 0; i < 3; ++i) {
            insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, rawData);
        }
    }

    private void insertSuccessData() throws Exception {
        final Map<String, Object> successData = new HashMap<String, Object>();
        //insert 10 successful calls for this BSC
        successData.put(HIER3_ID, BSCA01_HIER3_ID);
        successData.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusHours(1));
        successData.put(NO_OF_SUCCESSES, 10);
        insertRow(TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_SUC_15MIN, successData);

        //put some values outside of time range
        successData.clear();
        successData.put(HIER3_ID, BSCA01_HIER3_ID);
        successData.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinus48Hours());
        successData.put(NO_OF_SUCCESSES, 5);
        insertRow(TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_SUC_15MIN, successData);

        //put some successful calls for a different BSC
        successData.clear();
        successData.put(HIER3_ID, TEST_VALUE_GSM_HIER3_ID_BSC2);
        successData.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusHours(1));
        successData.put(NO_OF_SUCCESSES, 15);
        insertRow(TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_SUC_15MIN, successData);
    }

    private void insertRowsIntoAggregationView(final long hier3Id, final long hier321Id, final String datetime,
            final int instances) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(HIER3_ID, Long.toString(hier3Id));
        valuesForTable.put(HIER321_ID, Long.toString(hier321Id));
        valuesForTable.put(NO_OF_ERRORS, instances);
        valuesForTable.put(DATETIME_ID, datetime);
        valuesForTable.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_ERR_15MIN, valuesForTable);

        valuesForTable.put(CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_ERR_15MIN, valuesForTable);
    }

    private void insertEventTypeTable() throws Exception {
        Map<String, Object> columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE = new HashMap<String, Object>();
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.put(CATEGORY_ID_DESC, GSM_CALL_DROP_CATEGORY_ID_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_EVENTTYPE, columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE);

        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE = new HashMap<String, Object>();
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.put(CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER);
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.put(CATEGORY_ID_DESC, GSM_CALL_SETUP_FAILURES_CATEGORY_ID_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_EVENTTYPE, columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE);
    }

    private void insertDataIntoTacGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    public static long TEST_VALUE_GSM_HIER321_ID_CELL1 = 11111111111111L;

    public static long TEST_VALUE_GSM_HIER321_ID_CELL2 = 22222222222222L;

    public static long TEST_VALUE_GSM_HIER321_ID_CELL3 = 33333333333333L;
}
