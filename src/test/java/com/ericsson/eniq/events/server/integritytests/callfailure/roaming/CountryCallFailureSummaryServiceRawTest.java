package com.ericsson.eniq.events.server.integritytests.callfailure.roaming;

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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.roaming.CountryCallFailureSummaryService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.CountryCallFailureSummaryQueryResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class CountryCallFailureSummaryServiceRawTest extends BaseDataIntegrityTest<CountryCallFailureSummaryQueryResult> {

    private CountryCallFailureSummaryService countryCallFailureSummaryService;

    @Before
    public void onSetUp() throws Exception {
        countryCallFailureSummaryService = new CountryCallFailureSummaryService();
        attachDependencies(countryCallFailureSummaryService);
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
        assertThat(result.getNumberOfFailures(), is(4));
        assertThat(result.getNumberOfImpactedSubscribers(), is(1));
        assertThat(result.getFailureRatio(), is(22.22));

        //Verifying result for call drop
        result = results.get(1);

        assertThat(result.getCountry(), is("USA"));
        assertThat(result.getCategoryId(), is(0));
        assertThat(result.getCategoryDescription(), is(GSM_CALL_DROP_CATEGORY_ID_DESC));
        assertThat(result.getNumberOfFailures(), is(4));
        assertThat(result.getNumberOfImpactedSubscribers(), is(1));
        assertThat(result.getFailureRatio(), is(22.22));
    }

    private void insertDataIntoTacGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    private void insertEventData(final String dateTime) throws Exception {
        // Call drop roaming call with USA MCC
        insertData(GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER, MCC_USA, dateTime, TEST_VALUE_IMSI_1, 1, 4, SAMPLE_TAC);

        // Call setup failures roaming call with USA MCC
        insertData(GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER, MCC_USA, dateTime, TEST_VALUE_IMSI_1, 1, 4,
                SAMPLE_TAC);

        // Call drop non roaming call with USA MCC
        insertData(GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER, MCC_USA, dateTime, TEST_VALUE_IMSI_1, 0, 4, SAMPLE_TAC);

        // Call setup failures non roaming call with USA MCC
        insertData(GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER, MCC_USA, dateTime, TEST_VALUE_IMSI_1, 0, 4,
                SAMPLE_TAC);

        insertData(GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER, MCC_FOR_NORWAY, dateTime, TEST_VALUE_IMSI_2, 1, 1, SAMPLE_TAC);

        insertData(GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER, MCC_FOR_NORWAY, dateTime, TEST_VALUE_IMSI_2, 1, 1,
                SAMPLE_TAC);

        insertData(GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER, MCC_FOR_NORWAY, dateTime, TEST_VALUE_IMSI_2, 0, 1, SAMPLE_TAC);

        insertData(GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER, MCC_FOR_NORWAY, dateTime, TEST_VALUE_IMSI_2, 0, 1,
                SAMPLE_TAC);

    }

    private void insertData(final int categoryId, final String mcc, final String time, final long imsi,
            final int roaming, final int instances, final int tac) throws SQLException {
        for (int i = 0; i < instances; i++) {
            final Map<String, Object> dataForEventTable = new HashMap<String, Object>();

            dataForEventTable.put(IMSI_MCC, mcc);
            dataForEventTable.put(IMSI, imsi);
            dataForEventTable.put(CATEGORY_ID, categoryId);
            dataForEventTable.put(DATETIME_ID, time);
            dataForEventTable.put(ROAMING, roaming);
            dataForEventTable.put(TAC, tac);
            insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataForEventTable);
        }
    }

    private void insertLookupData() throws Exception {
        final Map<String, Object> tableData = new HashMap<String, Object>();

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
        //Roaming calls with USA MCC
        for (int i = 0; i < 10; ++i) {
            successData.put(IMSI_MCC, MCC_USA);
            successData.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinus2Minutes());
            successData.put(TAC, SAMPLE_TAC);
            successData.put(ROAMING, 1);
            insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, successData);
        }

        //put some entries of non roaming calls
        successData.clear();
        for (int i = 0; i < 3; ++i) {
            successData.put(IMSI_MCC, MCC_USA);
            successData.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinus30Minutes());
            successData.put(TAC, SAMPLE_TAC);
            successData.put(ROAMING, 0);
            insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, successData);
        }

        //Roaming calls with NORWAY MCC
        successData.clear();
        for (int i = 0; i < 15; ++i) {
            successData.put(IMSI_MCC, MCC_FOR_NORWAY);
            successData.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinus2Minutes());
            successData.put(TAC, SAMPLE_TAC);
            successData.put(ROAMING, 1);
            insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, successData);
        }
    }

    private void createTables() throws Exception {
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

        ReplaceTablesWithTempTablesTemplateUtils.addTableNameToReplace("DIM_E_GSM_CFA_EVENTTYPE",
                TEMP_DIM_E_GSM_CFA_EVENTTYPE);
    }

    private static final long TEST_VALUE_IMSI_1 = 9812326598L;

    private static final long TEST_VALUE_IMSI_2 = 1212326512L;

    public static final String TEMP_EVENT_E_GSM_CFA_SUC_RAW = "#EVENT_E_GSM_CFA_SUC_RAW";
}
