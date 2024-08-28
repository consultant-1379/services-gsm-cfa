/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.dataconnection.cc;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.resources.gsm.GSMApplicationConstants.*;
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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.dataconnection.cc.SubscriberCCService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.dataconnection.CauseCodePieChartResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ejoegaf
 * @since 2012
 *
 */
public class IMSICauseCodeRawTest extends BaseDataIntegrityTest<CauseCodePieChartResult> {

    private static final String TEST_VALUE_TBF_RELEASE_CAUSE_0 = "0";

    private static final String TEST_VALUE_TBF_RELEASE_CAUSE_0_DESC = "Normal release";

    private static final String TEST_VALUE_TBF_RELEASE_CAUSE_2 = "2";

    private static final String TEST_VALUE_TBF_RELEASE_CAUSE_2_DESC = "TBF released due to missing radio contact or removal of resources (no response from BTS after Immediate Assignment have been sent, timing advance channel faulty, no answer from the MS, essential channel removed)";

    private static final String TEST_VALUE_TBF_RELEASE_CAUSE_3 = "3";

    private static final String TEST_VALUE_TBF_RELEASE_CAUSE_3_DESC = "TBF released due to N3105 DL (too many consecutive unanswered polling requests), and TBF released due to N3103 UL (no reply from the MS after sending the final PACKET UPLINK ACK/NACK message)";

    private static final String TEST_VALUE_TBF_RELEASE_CAUSE_4 = "4";

    private static final String TEST_VALUE_TBF_RELEASE_CAUSE_4_DESC = "TBF released due to Flush";

    private static final String TEST_VALUE_IMSI = "460006082013326";

    private static final String TEST_VALUE_IMSI_2 = "460006082013327";

    private static final int TEST_VALUE_TAC = 100100;

    private SubscriberCCService imsiService;

    @Before
    public void setup() throws Exception {
        createEventTable();
        createLookupTables();
        insertDataIntoTacGroupTable();
        insertAllLookupData();
        insertEventData();
    }

    @Test
    public void testIMSIFiveMinuteQueryWithSuccessAndErrorCCs() throws URISyntaxException, Exception {
        final String result = runFiveMinCauseCodeQuery(DATA_CONNECTION_SUCCESS_CAUSE_CODE_ID_LABEL + ",0,4");
        verifyMixedResult(result);
    }

    @Test
    public void testIMSIFiveMinuteQueryWithSuccessCCOnly() throws URISyntaxException, Exception {
        final String result = runFiveMinCauseCodeQuery(DATA_CONNECTION_SUCCESS_CAUSE_CODE_ID_LABEL);
        verifyResultWithSucessOnly(result);
    }

    @Test
    public void testIMSIFiveMinuteQueryWithErrorCCsOnly() throws URISyntaxException, Exception {
        final String result = runFiveMinCauseCodeQuery("0,4");
        verifyResultWithErrorCCSOnly(result);
    }

    private String runFiveMinCauseCodeQuery(final String ccList) throws URISyntaxException, Exception {
        imsiService = new SubscriberCCService();
        attachDependencies(imsiService);
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TIME_QUERY_PARAM, FIVE_MINUTES);
        requestParameters.add(IMSI_PARAM, TEST_VALUE_IMSI);
        requestParameters.add(TYPE_PARAM, TYPE_IMSI);
        requestParameters.add(CAUSE_CODE_ID_LIST, ccList);
        return runQuery(imsiService, requestParameters);
    }

    private void verifyMixedResult(final String json) throws Exception {
        assertJSONSucceeds(json);
        final List<CauseCodePieChartResult> results = getTranslator().translateResult(json,
                CauseCodePieChartResult.class);

        assertThat(results.size(), is(3));//success included, and 2 other cause codes
        assertThat(results.get(0).getCauseCodeId(), is(DATA_CONNECTION_SUCCESS_CAUSE_CODE_ID_LABEL));
        assertThat(results.get(0).getCauseCodeDesc(), is(SUCCESSES));
        assertThat(results.get(0).getNoOccurrences(), is(9));
        assertThat(results.get(0).getNoImpactedSubscribers(), is(0));

        assertThat(results.get(1).getCauseCodeId(), is(TEST_VALUE_TBF_RELEASE_CAUSE_0));
        assertThat(results.get(1).getCauseCodeDesc(), is(TEST_VALUE_TBF_RELEASE_CAUSE_0_DESC));
        assertThat(results.get(1).getNoOccurrences(), is(4));
        assertThat(results.get(1).getNoImpactedSubscribers(), is(1));

        assertThat(results.get(2).getCauseCodeId(), is(TEST_VALUE_TBF_RELEASE_CAUSE_4));
        assertThat(results.get(2).getCauseCodeDesc(), is(TEST_VALUE_TBF_RELEASE_CAUSE_4_DESC));
        assertThat(results.get(2).getNoOccurrences(), is(3));
        assertThat(results.get(2).getNoImpactedSubscribers(), is(1));
    }

    private void verifyResultWithSucessOnly(final String json) throws Exception {
        assertJSONSucceeds(json);
        final List<CauseCodePieChartResult> results = getTranslator().translateResult(json,
                CauseCodePieChartResult.class);

        assertThat(results.size(), is(1));//success included, and no other cause codes
        assertThat(results.get(0).getCauseCodeId(), is(DATA_CONNECTION_SUCCESS_CAUSE_CODE_ID_LABEL));
        assertThat(results.get(0).getCauseCodeDesc(), is(SUCCESSES));
        assertThat(results.get(0).getNoOccurrences(), is(9));
        assertThat(results.get(0).getNoImpactedSubscribers(), is(0));
    }

    private void verifyResultWithErrorCCSOnly(final String json) throws Exception {
        assertJSONSucceeds(json);
        final List<CauseCodePieChartResult> results = getTranslator().translateResult(json,
                CauseCodePieChartResult.class);

        assertThat(results.size(), is(2));

        assertThat(results.get(0).getCauseCodeId(), is(TEST_VALUE_TBF_RELEASE_CAUSE_0));
        assertThat(results.get(0).getCauseCodeDesc(), is(TEST_VALUE_TBF_RELEASE_CAUSE_0_DESC));
        assertThat(results.get(0).getNoOccurrences(), is(4));
        assertThat(results.get(0).getNoImpactedSubscribers(), is(1));

        assertThat(results.get(1).getCauseCodeId(), is(TEST_VALUE_TBF_RELEASE_CAUSE_4));
        assertThat(results.get(1).getCauseCodeDesc(), is(TEST_VALUE_TBF_RELEASE_CAUSE_4_DESC));
        assertThat(results.get(1).getNoOccurrences(), is(3));
        assertThat(results.get(1).getNoImpactedSubscribers(), is(1));
    }

    private void insertDataIntoTacGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    private void insertEventData() throws Exception {
        final String dateTime2mins = DateTimeUtilities.getDateTimeMinus2Minutes();
        final String dateTime3mins = DateTimeUtilities.getDateTimeMinus3Minutes();
        final String dateTime48Hours = DateTimeUtilities.getDateTimeMinus48Hours();
        //success events - test they are not counted as errors
        insertData(true, TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_0, dateTime2mins, 1, TEST_VALUE_TAC);
        insertData(true, TEST_VALUE_IMSI_2, TEST_VALUE_TBF_RELEASE_CAUSE_0, dateTime2mins, 2, TEST_VALUE_TAC);
        insertData(true, TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_0, dateTime3mins, 3, TEST_VALUE_TAC);
        insertData(true, TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_0, dateTime48Hours, 3, TEST_VALUE_TAC);
        insertData(true, TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_0, dateTime3mins, 3, SAMPLE_EXCLUSIVE_TAC);
        insertData(true, TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_2, dateTime2mins, 1, TEST_VALUE_TAC);
        insertData(true, TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_4, dateTime2mins, 1, TEST_VALUE_TAC);

        insertData(false, TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_0, dateTime2mins, 1, TEST_VALUE_TAC);//should be in result
        insertData(false, TEST_VALUE_IMSI_2, TEST_VALUE_TBF_RELEASE_CAUSE_0, dateTime2mins, 2, TEST_VALUE_TAC);//not expected in result - different imsi
        insertData(false, TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_0, dateTime3mins, 3, TEST_VALUE_TAC);//should be in result
        insertData(false, TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_0, dateTime48Hours, 9, TEST_VALUE_TAC); //not expected in result - outside time limit
        insertData(false, TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_4, dateTime3mins, 3, SAMPLE_EXCLUSIVE_TAC);//should be in result - tacs not excluded for explicit imsi request
        insertData(false, TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_3, dateTime2mins, 1, TEST_VALUE_TAC);//not expected in result - cc id not in list
    }

    private void insertData(final boolean successEvents, final String imsi, final String causeCode, final String time,
            final int instances, final int tac) throws SQLException {
        final Map<String, Object> dataForEventTable = new HashMap<String, Object>();
        if (successEvents) {
            dataForEventTable.put(NO_OF_SUCCESSES, instances);
            dataForEventTable.put(NO_OF_ERRORS, 0);
        } else {
            dataForEventTable.put(NO_OF_SUCCESSES, 0);
            dataForEventTable.put(NO_OF_ERRORS, instances);
        }
        dataForEventTable.put(TAC, tac);
        dataForEventTable.put(TBF_RELEASE_CAUSE, causeCode);
        dataForEventTable.put(DATETIME_ID, time);
        dataForEventTable.put(IMSI, imsi);
        insertRow(TEMP_EVENT_E_GSM_PS_ALL_RAW, dataForEventTable);
    }

    private void createLookupTables() throws Exception {
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_TBF_RELEASE_CAUSE, "DIM_E_GSM_PS_TBF_RELEASE_CAUSE",
                TBF_RELEASE_CAUSE, TBF_RELEASE_CAUSE_DESC);
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
        insertUrgencyConditionLookupData();
    }

    private void insertRowToUrgencyConditionTable(final String urgencyCondition,
            final String urgencyConditionDescription) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(TBF_RELEASE_CAUSE, urgencyCondition);
        valuesForTable.put(TBF_RELEASE_CAUSE_DESC, urgencyConditionDescription);
        insertRow(TEMP_DIM_E_GSM_PS_TBF_RELEASE_CAUSE, valuesForTable);
    }

    private void insertUrgencyConditionLookupData() throws SQLException {
        insertRowToUrgencyConditionTable(TEST_VALUE_TBF_RELEASE_CAUSE_0, TEST_VALUE_TBF_RELEASE_CAUSE_0_DESC);
        insertRowToUrgencyConditionTable(TEST_VALUE_TBF_RELEASE_CAUSE_2, TEST_VALUE_TBF_RELEASE_CAUSE_2_DESC);
        insertRowToUrgencyConditionTable(TEST_VALUE_TBF_RELEASE_CAUSE_3, TEST_VALUE_TBF_RELEASE_CAUSE_3_DESC);
        insertRowToUrgencyConditionTable(TEST_VALUE_TBF_RELEASE_CAUSE_4, TEST_VALUE_TBF_RELEASE_CAUSE_4_DESC);
    }

    private void createEventTable() throws Exception {
        final Collection<String> columnsForEventTable = new ArrayList<String>();

        columnsForEventTable.add(NO_OF_ERRORS);
        columnsForEventTable.add(NO_OF_SUCCESSES);
        columnsForEventTable.add(TBF_RELEASE_CAUSE);
        columnsForEventTable.add(TAC);
        columnsForEventTable.add(IMSI);
        columnsForEventTable.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_PS_ALL_RAW, columnsForEventTable);
    }

}
