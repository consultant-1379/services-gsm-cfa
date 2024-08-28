/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.callfailure.roaming;

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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.roaming.CountryCallFailureSummaryService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.CountryCallFailureSummaryQueryResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class CountryCallFailureSummaryServiceAggTest extends BaseDataIntegrityTest<CountryCallFailureSummaryQueryResult> {

    private static final String TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_ERR_15MIN = "#EVENT_E_GSM_CFA_HIER321_EVENTID_ERR_15MIN";

    private static final String TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_SUC_15MIN = "#EVENT_E_GSM_CFA_HIER321_EVENTID_SUC_15MIN";

    private CountryCallFailureSummaryService countryCallFailureSummaryService;

    private static final int IMSI_TEST_VALUE_1 = 1;

    /**
     * 1. Create tables. 2. Insert test data to the tables.
     * 
     * @throws Exception
     */
    @Before
    public void onSetUp() throws Exception {
        countryCallFailureSummaryService = new CountryCallFailureSummaryService();
        attachDependencies(countryCallFailureSummaryService);
        createTables();
        insertDataIntoTacGroupTable();
        insertData();
    }

    @Test
    public void testGetEventSummaryData() throws Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        requestParameters.add(TIME_QUERY_PARAM, ONE_DAY);
        requestParameters.add(MCC, MCC_USA);
        requestParameters.add(COUNTRY, "USA");

        final String result = runQuery(countryCallFailureSummaryService, requestParameters);
        verifyResult(result);
    }

    private void verifyResult(final String json) throws Exception {
        assertJSONSucceeds(json);
        final List<CountryCallFailureSummaryQueryResult> results = getTranslator().translateResult(json,
                CountryCallFailureSummaryQueryResult.class);

        assertThat(results.size(), is(2));
        validateAgainstGridDefinition(json, "GSM_ROAMING_ANALYSIS_SUMMARY_BY_COUNTRY");
        CountryCallFailureSummaryQueryResult result = results.get(0);

        assertThat(result.getCountry(), is("USA"));
        assertThat(result.getCategoryId(), is(1));
        assertThat(result.getCategoryDescription(), is(GSM_CALL_SETUP_FAILURES_CATEGORY_ID_DESC));
        assertThat(result.getNumberOfFailures(), is(3));
        assertThat(result.getNumberOfImpactedSubscribers(), is(1));
        assertThat(result.getFailureRatio(), is(18.75));

        result = results.get(1);

        assertThat(result.getCountry(), is("USA"));
        assertThat(result.getCategoryId(), is(0));
        assertThat(result.getCategoryDescription(), is(GSM_CALL_DROP_CATEGORY_ID_DESC));
        assertThat(result.getNumberOfFailures(), is(3));
        assertThat(result.getNumberOfImpactedSubscribers(), is(1));
        assertThat(result.getFailureRatio(), is(18.75));
    }

    private void createTables() throws Exception {
        final Collection<String> columnsForEventTable = new ArrayList<String>();
        columnsForEventTable.add(IMSI_MCC);
        columnsForEventTable.add(NO_OF_ERRORS);
        columnsForEventTable.add(CATEGORY_ID);
        columnsForEventTable.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_MCC_MNC_ROAM_15MIN, columnsForEventTable);

        columnsForEventTable.clear();
        columnsForEventTable.add(IMSI_MCC);
        columnsForEventTable.add(DATETIME_ID);
        columnsForEventTable.add(NO_OF_SUCCESSES);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_MCC_MNC_ROAM_SUC_15MIN, columnsForEventTable);

        final Collection<String> columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE = new ArrayList<String>();
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.add(CATEGORY_ID_DESC);
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.add(CATEGORY_ID);
        createTemporaryTable(TEMP_DIM_E_GSM_CFA_EVENTTYPE, columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE);

        final Collection<String> columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW = new ArrayList<String>();
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(IMSI);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(DATETIME_ID);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(CATEGORY_ID);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(IMSI_MCC);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(ROAMING);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(TAC);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);

        final Collection<String> columnsFor_TEMP_EVENT_E_GSM_CFA_SUC_RAW = new ArrayList<String>();
        columnsFor_TEMP_EVENT_E_GSM_CFA_SUC_RAW.add(IMSI_MCC);
        columnsFor_TEMP_EVENT_E_GSM_CFA_SUC_RAW.add(DATETIME_ID);
        columnsFor_TEMP_EVENT_E_GSM_CFA_SUC_RAW.add(IMSI_MCC);
        columnsFor_TEMP_EVENT_E_GSM_CFA_SUC_RAW.add(ROAMING);
        columnsFor_TEMP_EVENT_E_GSM_CFA_SUC_RAW.add(TAC);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_SUC_RAW, columnsFor_TEMP_EVENT_E_GSM_CFA_SUC_RAW);

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

    private void insertData() throws Exception {

        insertIntoRawTable();

        insertEventTypeTable();

        insertSuccessData();

        final String dateTime = DateTimeUtilities.getDateTimeMinusHours(1);

        insertRowsIntoAggregationView(MCC_USA, dateTime, 3);
        insertRowsIntoAggregationView(MCC_FOR_NORWAY, dateTime, 3);

    }

    private void insertIntoRawTable() throws Exception {
        final Map<String, Object> rawData = new HashMap<String, Object>();
        rawData.put(IMSI_MCC, MCC_USA);
        rawData.put(IMSI, IMSI_TEST_VALUE_1);
        rawData.put(TAC, SAMPLE_TAC);
        rawData.put(ROAMING, 1);
        rawData.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusHours(1));
        rawData.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);

        for (int i = 0; i < 3; ++i) {
            insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, rawData);
        }

        // Adding non roaming call drop failures
        rawData.remove(ROAMING);
        rawData.put(ROAMING, 0);

        for (int i = 0; i < 3; ++i) {
            insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, rawData);
        }

        //Adding Roaming Call Setup Failures of different IMSI
        rawData.clear();
        rawData.put(IMSI_MCC, MCC_USA);
        rawData.put(IMSI, IMSI_TEST_VALUE_1);
        rawData.put(TAC, SAMPLE_TAC);
        rawData.put(ROAMING, 1);
        rawData.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusHours(1));
        rawData.put(CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER);
        for (int i = 0; i < 3; ++i) {
            insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, rawData);
        }

        // Adding non roaming call Setup failures
        rawData.remove(ROAMING);
        rawData.put(ROAMING, 0);

        for (int i = 0; i < 3; ++i) {
            insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, rawData);
        }
        //Adding roaming calls with MCC_MORWAY
        rawData.clear();
        rawData.put(IMSI_MCC, MCC_FOR_NORWAY);
        rawData.put(IMSI, IMSI_TEST_VALUE_1);
        rawData.put(TAC, SAMPLE_TAC);
        rawData.put(ROAMING, 1);
        rawData.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusHours(1));
        rawData.put(CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER);
        for (int i = 0; i < 3; ++i) {
            insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, rawData);
        }

    }

    private void insertSuccessData() throws Exception {
        final Map<String, Object> successData = new HashMap<String, Object>();
        //insert 10 successful calls for this BSC
        successData.put(IMSI_MCC, MCC_USA);
        successData.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusHours(1));
        successData.put(NO_OF_SUCCESSES, 10);
        insertRow(TEMP_EVENT_E_GSM_CFA_MCC_MNC_ROAM_SUC_15MIN, successData);

    }

    private void insertRowsIntoAggregationView(final String mcc, final String datetime, final int instances)
            throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(IMSI_MCC, mcc);
        valuesForTable.put(NO_OF_ERRORS, instances);
        valuesForTable.put(DATETIME_ID, datetime);
        valuesForTable.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_MCC_MNC_ROAM_15MIN, valuesForTable);

        valuesForTable.put(CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_MCC_MNC_ROAM_15MIN, valuesForTable);
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
