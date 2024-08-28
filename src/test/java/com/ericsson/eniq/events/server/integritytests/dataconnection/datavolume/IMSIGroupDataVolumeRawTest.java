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
public class IMSIGroupDataVolumeRawTest extends BaseDataIntegrityTest<SubscriberDataVolumeAnalysisResult> {

    private SubscriberDataVolumeService service;

    private static final String TEST_IMSI_1 = "46000608201336";

    private static final String TEST_IMSI_2 = "53000608201337";

    private static final String TEST_IMSI_3 = "1298608201337";

    private static final String TEST_VALUE_IMSIGROUP1 = "IMSIGroup1";

    private static final String TEST_VALUE_IMSIGROUP2 = "IMSIGroup2";

    private static final double TEST_DOWNLOAD_DATAVOL = 100.00;

    private static final int TEST_DOWNLOAD_DURATION = 20;

    private static final double TEST_UPLOAD_DATAVOL = 200.00;

    private static final int TEST_UPLOAD_DURATION = 10;

    private static final double TEST_TOTAL_DATAVOL = 400.00;

    private static final int TEST_TOTAL_DURATION = 40;

    @Before
    public void onSetUp() throws Exception {
        service = new SubscriberDataVolumeService();
        attachDependencies(service);
        createTable();
        createGroupImsiTable();
        insertDataIntoTacGroupTable();
        insertGroupData();
        insertEventData();
    }

    @Test
    public void testThirtyMinuteQuery() throws Exception {

        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TIME_QUERY_PARAM, "30");
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TYPE_PARAM, IMSI);
        requestParameters.add(GROUP_NAME_PARAM, TEST_VALUE_IMSIGROUP1);
        final String result = runQuery(service, requestParameters);
        verifyResult(result);
    }

    @Test
    public void testTwoHourQuery() throws Exception {

        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(TIME_QUERY_PARAM, "120");
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TYPE_PARAM, IMSI);
        requestParameters.add(GROUP_NAME_PARAM, TEST_VALUE_IMSIGROUP1);
        final String result = runQuery(service, requestParameters);
        verifyResult(result);
    }

    private void verifyResult(final String json) throws Exception {
        assertJSONSucceeds(json);
        final ResultTranslator<SubscriberDataVolumeAnalysisResult> rt = getTranslator();
        final List<SubscriberDataVolumeAnalysisResult> GroupTestResult = rt.translateResult(json,
                SubscriberDataVolumeAnalysisResult.class);

        validateAgainstGridDefinition(json, "GSM_PS_SUBSCRIBER_DATAVOLUME_ANALYSIS_BY_IMSI_GROUP");
        assertThat(GroupTestResult.size(), is(2));

        final SubscriberDataVolumeAnalysisResult firstResult = GroupTestResult.get(1);
        // This should get the summed results for imsi 46000608201336 which was
        // has 3 enteries in IMSIGroup1
        assertThat(firstResult.getImsi(), is(TEST_IMSI_1));
        assertThat("Expected Download Datavol is:", firstResult.getDownLoadDataVol(), is(300.00));
        assertThat("Expected Download Duration is:", firstResult.getDownLoadDuration(), is(6));
        assertThat("Expected Download Throughput is:", firstResult.getDownLoadThroughput(), is(50.00));
        assertThat("Expected Upload Datavol is:", firstResult.getUpLoadDataVol(), is(600.00));
        assertThat("Expected Upload Duration is:", firstResult.getUpLoadDuration(), is(3));
        assertThat("Expected Upload Throughput is:", firstResult.getUpLoadThroughput(), is(200.00));
        assertThat("Expected Total Datavol is:", firstResult.getTotalDataVol(), is(1200.00));
        assertThat("Expected Total Duration is:", firstResult.getTotalDuration(), is(12));
        assertThat("Expected Total Throughput is:", firstResult.getTotalThroughput(), is(100.00));

        // This should get the summed results for imsi 53000608201337 which now
        // has only 1 entry in IMSIGroup1 as one of these is an exclusive tac
        // and
        // therefore should not be included
        final SubscriberDataVolumeAnalysisResult secondResult = GroupTestResult.get(0);
        assertThat(secondResult.getImsi(), is(TEST_IMSI_2));
        assertThat("Expected Download Datavol is:", secondResult.getDownLoadDataVol(), is(100.00));
        assertThat("Expected Download Duration is:", secondResult.getDownLoadDuration(), is(2));
        assertThat("Expected Download Throughput is:", secondResult.getDownLoadThroughput(), is(50.00));
        assertThat("Expected Upload Datavol is:", secondResult.getUpLoadDataVol(), is(200.00));
        assertThat("Expected Upload Duration is:", secondResult.getUpLoadDuration(), is(1));
        assertThat("Expected Upload Throughput is:", secondResult.getUpLoadThroughput(), is(200.00));
        assertThat("Expected Total Datavol is:", secondResult.getTotalDataVol(), is(400.00));
        assertThat("Expected Total Duration is:", secondResult.getTotalDuration(), is(4));
        assertThat("Expected Total Throughput is:", secondResult.getTotalThroughput(), is(100.00));

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

    private void createGroupImsiTable() throws Exception {
        final Map<String, Nullable> columnsForGroupTable = new HashMap<String, Nullable>();
        columnsForGroupTable.put(GROUP_NAME, Nullable.CAN_BE_NULL);
        columnsForGroupTable.put(IMSI, Nullable.CAN_BE_NULL);
        createTemporaryTable(TEMP_GROUP_TYPE_E_IMSI, columnsForGroupTable);

    }

    private void insertDataIntoTacGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    private void insertGroupData() throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(GROUP_NAME, TEST_VALUE_IMSIGROUP1);
        valuesForTable.put(IMSI, TEST_IMSI_1);
        insertRow(TEMP_GROUP_TYPE_E_IMSI, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(GROUP_NAME, TEST_VALUE_IMSIGROUP1);
        valuesForTable.put(IMSI, TEST_IMSI_2);
        insertRow(TEMP_GROUP_TYPE_E_IMSI, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(GROUP_NAME, TEST_VALUE_IMSIGROUP2);
        valuesForTable.put(IMSI, TEST_IMSI_3);
        insertRow(TEMP_GROUP_TYPE_E_IMSI, valuesForTable);
    }

    private void insertEventData() throws Exception {
        final String dateTimeNowMinus15Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(15 + GSM_DATA_CONNECTION_LATENCY_ON_THIRTY_MIN_QUERY);

        final String dateTimeNowMinus180Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(180 + GSM_DATA_CONNECTION_LATENCY_ON_THIRTY_MIN_QUERY);

        insertRowToPSRAW(TEST_IMSI_1, SAMPLE_TAC, dateTimeNowMinus15Mins, 3);
        insertRowToPSRAW(TEST_IMSI_2, SAMPLE_EXCLUSIVE_TAC, dateTimeNowMinus15Mins, 1);
        insertRowToPSRAW(TEST_IMSI_2, SAMPLE_TAC, dateTimeNowMinus15Mins, 1);
        insertRowToPSRAW(TEST_IMSI_3, SAMPLE_TAC, dateTimeNowMinus15Mins, 3);
        insertRowToPSRAW(TEST_IMSI_2, SAMPLE_TAC, dateTimeNowMinus180Mins, 1);
        // outside time range so should not be included

    }

    private void insertRowToPSRAW(final String imsi, final int tac, final String date, final int instances)
            throws SQLException {
        for (int i = 0; i < instances; i++) {
            final Map<String, Object> valuesForTable = new HashMap<String, Object>();
            valuesForTable.put(IMSI, imsi);
            valuesForTable.put(DATETIME_ID, date);
            valuesForTable.put(DOWNLOAD_DATA_VOLUME, TEST_DOWNLOAD_DATAVOL);
            valuesForTable.put(DOWNLOAD_DURATION, TEST_DOWNLOAD_DURATION);
            valuesForTable.put(UPLOAD_DATA_VOLUME, TEST_UPLOAD_DATAVOL);
            valuesForTable.put(UPLOAD_DURATION, TEST_UPLOAD_DURATION);
            valuesForTable.put(TOTAL_DATA_VOLUME, TEST_TOTAL_DATAVOL);
            valuesForTable.put(TOTAL_DURATION, TEST_TOTAL_DURATION);
            valuesForTable.put(TAC, tac);
            insertRow(TEMP_EVENT_E_GSM_PS_ALL_RAW, valuesForTable);
        }

    }

}