/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.dataconnection.datavolume;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.integritytests.dataconnection.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.dataconnection.datavolume.SubscriberDataVolumeService;
import com.ericsson.eniq.events.server.test.queryresults.ResultTranslator;
import com.ericsson.eniq.events.server.test.queryresults.gsm.dataconnection.SubscriberDataVolumeAnalysisResult;
import com.ericsson.eniq.events.server.test.schema.Nullable;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ETHOMIT
 * @since 2012
 * 
 */
public class ImsiDataVolumeRawTest extends BaseDataIntegrityTest<SubscriberDataVolumeAnalysisResult> {

    private SubscriberDataVolumeService service;

    private static final String TEST_IMSI_1 = "460000748502770";

    private static final String TEST_IMSI_2 = "53000608201337";

    private static final String TEST_IMSI_3 = "83006776201344";

    @Before
    public void onSetUp() throws Exception {
        service = new SubscriberDataVolumeService();
        attachDependencies(service);
        createTable();
        insertDataIntoTacGroupTable();
        insertData();
    }

    @Test
    public void testThirtyMinuteQuery() throws Exception {

        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TYPE_PARAM, IMSI);
        requestParameters.add(IMSI_PARAM_UPPER_CASE, TEST_IMSI_1);
        final String result = runQuery(service, requestParameters);
        verifyResult(result);
    }

    @Test
    public void testTwoHourQuery() throws Exception {

        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TIME_QUERY_PARAM, "120");
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TYPE_PARAM, IMSI);
        requestParameters.add(IMSI_PARAM_UPPER_CASE, TEST_IMSI_1);
        final String result = runQuery(service, requestParameters);
        verifyResult(result);
    }

    @Test
    // this query should return the exclusive TAC result
    public void testTacQuery() throws URISyntaxException, Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TIME_QUERY_PARAM, THIRTY_MINUTES);
        requestParameters.add(TYPE_PARAM, IMSI);
        requestParameters.add(IMSI_PARAM_UPPER_CASE, TEST_IMSI_2);
        final String result = runQuery(service, requestParameters);
        final List<SubscriberDataVolumeAnalysisResult> TacTestResults = getTranslator().translateResult(result,
                SubscriberDataVolumeAnalysisResult.class);
        assertThat(TacTestResults.size(), is(1));
    }

    private void verifyResult(final String json) throws Exception {
        assertJSONSucceeds(json);
        final ResultTranslator<SubscriberDataVolumeAnalysisResult> rt = getTranslator();
        final List<SubscriberDataVolumeAnalysisResult> rankingResult = rt.translateResult(json,
                SubscriberDataVolumeAnalysisResult.class);

        validateAgainstGridDefinition(json, "GSM_PS_SUBSCRIBER_DATAVOLUME_ANALYSIS_BY_IMSI");

        assertThat("There should be exactly 1 result!", rankingResult.size(), is(1));

        final List<SubscriberDataVolumeAnalysisResult> TestResults = getTranslator().translateResult(json,
                SubscriberDataVolumeAnalysisResult.class);

        final SubscriberDataVolumeAnalysisResult TestResult = TestResults.get(0);

        assertThat(TestResult.getImsi(), is(TEST_IMSI_1));
        assertThat(TestResult.getDownLoadDataVol(), is(400.00));
        assertThat(TestResult.getDownLoadDuration(), is(50));
        assertThat(TestResult.getDownLoadThroughput(), is(8.00));
        assertThat(TestResult.getUpLoadDataVol(), is(600.00));
        assertThat(TestResult.getUpLoadDuration(), is(300));
        assertThat(TestResult.getUpLoadThroughput(), is(2.00));
        assertThat(TestResult.getTotalDataVol(), is(1000.00));
        assertThat(TestResult.getTotalDuration(), is(200));
        assertThat(TestResult.getUpLoadThroughput(), is(2.00));

    }

    private void createTable() throws Exception {

        final Map<String, Nullable> columnsForEventsTable = new HashMap<String, Nullable>();
        columnsForEventsTable.put(IMSI, Nullable.CAN_BE_NULL);
        columnsForEventsTable.put(DOWNLOAD_DATA_VOLUME, Nullable.CAN_BE_NULL);
        columnsForEventsTable.put(DOWNLOAD_DURATION, Nullable.CAN_BE_NULL);
        columnsForEventsTable.put(UPLOAD_DATA_VOLUME, Nullable.CAN_BE_NULL);
        columnsForEventsTable.put(UPLOAD_DURATION, Nullable.CAN_BE_NULL);
        columnsForEventsTable.put(TOTAL_DATA_VOLUME, Nullable.CAN_BE_NULL);
        columnsForEventsTable.put(TOTAL_DURATION, Nullable.CAN_BE_NULL);
        columnsForEventsTable.put(DATETIME_ID, Nullable.CAN_BE_NULL);
        columnsForEventsTable.put(TAC, Nullable.CANNOT_BE_NULL);
        createTemporaryTable(TEMP_EVENT_E_GSM_PS_ALL_RAW, columnsForEventsTable);
    }

    private void insertDataIntoTacGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    private void insertData() throws Exception {

        final String dateTimeNowMinus15Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(15 + GSM_DATA_CONNECTION_LATENCY_ON_THIRTY_MIN_QUERY);

        insertRowToPSRAW(TEST_IMSI_1, dateTimeNowMinus15Mins, SAMPLE_TAC, 200.0, 250, 300.0, 1500, 500.0, 1000);
        insertRowToPSRAW(TEST_IMSI_2, dateTimeNowMinus15Mins, SAMPLE_EXCLUSIVE_TAC, 100.0, 500, 180.0, 7000, 280.0,
                9000);
        insertRowToPSRAW(TEST_IMSI_1, dateTimeNowMinus15Mins, SAMPLE_TAC, 200.0, 250, 300.0, 1500, 500.0, 1000);
        insertRowToPSRAW(TEST_IMSI_3, dateTimeNowMinus15Mins, SAMPLE_TAC, 500.0, 360, 950.0, 1700, 600.0, 875);
    }

    private void insertRowToPSRAW(final String imsi, final String date, final int tac, final double downLoadDataVol,
            final double downLoadDuration, final double upLoadDataVol, final double upLoadDuration,
            final double totalDataVol, final double totalDuration) throws SQLException {

        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(IMSI, imsi);
        valuesForTable.put(DATETIME_ID, date);
        valuesForTable.put(TAC, tac);
        valuesForTable.put(DOWNLOAD_DATA_VOLUME, (downLoadDataVol));
        valuesForTable.put(DOWNLOAD_DURATION, (downLoadDuration));
        valuesForTable.put(UPLOAD_DATA_VOLUME, (upLoadDataVol));
        valuesForTable.put(UPLOAD_DURATION, (upLoadDuration));
        valuesForTable.put(TOTAL_DATA_VOLUME, (totalDataVol));
        valuesForTable.put(TOTAL_DURATION, (totalDuration));
        insertRow(TEMP_EVENT_E_GSM_PS_ALL_RAW, valuesForTable);

    }

}