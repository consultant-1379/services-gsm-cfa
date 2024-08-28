/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.dataconnection.ranking;

import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.dataconnection.ranking.SubscriberDataVolumeRankingService;
import com.ericsson.eniq.events.server.test.queryresults.ResultTranslator;
import com.ericsson.eniq.events.server.test.queryresults.gsm.dataconnection.SubscriberDataVolumeRankingResult;
import com.ericsson.eniq.events.server.test.schema.Nullable;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.MultivaluedMap;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.integritytests.dataconnection.ApplicationTestConstants.GSM_DATA_CONNECTION_LATENCY_ON_THIRTY_MIN_QUERY;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_GSM_PS_ALL_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_GROUP_TYPE_E_TAC;
import static org.junit.Assert.assertEquals;

/**
 * @author ejoegaf
 * @since 2011
 *
 */
public class SubscriberDataVolumeRankingServiceRawTest extends BaseDataIntegrityTest<SubscriberDataVolumeRankingResult> {

    private SubscriberDataVolumeRankingService subscriberDataVolumeRankingService;

    private static final long IMSI_NULL = -1;

    private static final long DATA_VOLUME_NULL = -1;

    private static final long TEST_IMSI_0 = 0;

    private static final long TEST_IMSI_1 = 460000400831255L;

    private static final long TEST_IMSI_2 = 460000469407326L;

    private static final long TEST_IMSI_3 = 460000322556069L;

    private static final long KILOBYTE_TO_BYTE_RATE = 1024;

    @Before
    public void onSetUp() throws Exception {
        subscriberDataVolumeRankingService = new SubscriberDataVolumeRankingService();
        attachDependencies(subscriberDataVolumeRankingService);
        createTable();
        insertDataIntoTacGroupTable();
        insertData();
    }

    /*
     * The expected outcome is for IMSI_3 to rank first, then IMSI_2 and finally IMSI_1
     */
    @Test
    public void testSubscriberRankingByDataVolume() throws Exception {

        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(DISPLAY_PARAM, GRID_PARAM);
        requestParameters.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        requestParameters.putSingle(TZ_OFFSET, "+0100");
        requestParameters.putSingle(MAX_ROWS, "10");
        final String json = runQuery(subscriberDataVolumeRankingService, requestParameters);
        final ResultTranslator<SubscriberDataVolumeRankingResult> resultTranslator = getTranslator();
        final List<SubscriberDataVolumeRankingResult> rankingResult = resultTranslator.translateResult(json,
                SubscriberDataVolumeRankingResult.class);

        final List<SubscriberDataVolumeRankingResult> expectedResult = new ArrayList<SubscriberDataVolumeRankingResult>();
        expectedResult.add(new SubscriberDataVolumeRankingResult(1, TEST_IMSI_3, 9.0, 9.0, 18.0));
        expectedResult.add(new SubscriberDataVolumeRankingResult(2, TEST_IMSI_2, 10.0, 7.0, 17.0));
        expectedResult.add(new SubscriberDataVolumeRankingResult(3, TEST_IMSI_1, 5.0, 10.0, 15.0));

        assertEquals(expectedResult, rankingResult);
    }

    private void createTable() throws Exception {

        final Map<String, Nullable> columnsForEventsTable = new HashMap<String, Nullable>();
        columnsForEventsTable.put(IMSI, Nullable.CAN_BE_NULL);
        columnsForEventsTable.put(DOWNLOAD_DATA_VOLUME, Nullable.CAN_BE_NULL);
        columnsForEventsTable.put(UPLOAD_DATA_VOLUME, Nullable.CAN_BE_NULL);
        columnsForEventsTable.put(TOTAL_DATA_VOLUME, Nullable.CAN_BE_NULL);
        columnsForEventsTable.put(DATETIME_ID, Nullable.CANNOT_BE_NULL);
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
        final String dateTimeNowMinus20Mins = DateTimeUtilities
                .getDateTimeMinusMinutes(20 + GSM_DATA_CONNECTION_LATENCY_ON_THIRTY_MIN_QUERY);

        insertRowToPSRAW(TEST_IMSI_0, dateTimeNowMinus15Mins, SAMPLE_TAC, 10, 7, 100);

        insertRowToPSRAW(TEST_IMSI_0, dateTimeNowMinus15Mins, SAMPLE_TAC, DATA_VOLUME_NULL, DATA_VOLUME_NULL,
                DATA_VOLUME_NULL);

        insertRowToPSRAW(TEST_IMSI_1, dateTimeNowMinus15Mins, SAMPLE_TAC, 5, 10, 15);

        insertRowToPSRAW(TEST_IMSI_2, dateTimeNowMinus15Mins, SAMPLE_TAC, 10, 7, 17);

        insertRowToPSRAW(TEST_IMSI_2, dateTimeNowMinus15Mins, SAMPLE_TAC, DATA_VOLUME_NULL, DATA_VOLUME_NULL,
                DATA_VOLUME_NULL);

        insertRowToPSRAW(TEST_IMSI_3, dateTimeNowMinus20Mins, SAMPLE_TAC, 9, 9, 18);

        insertRowToPSRAW(TEST_IMSI_1, dateTimeNowMinus15Mins, SAMPLE_EXCLUSIVE_TAC, 10, 10, 20);

        insertRowToPSRAW(IMSI_NULL, dateTimeNowMinus15Mins, SAMPLE_TAC, 100, 100, 200);

    }

    /**
     * 
     * @param imsi          IMSI will be NULL when imsi is -1
     * @param date
     * @param tac
     * @param downDataVol   unit is KB
     * @param upDataVol     unit is KB
     * @param totalDataVol  unit is KB
     * @throws SQLException
     */
    private void insertRowToPSRAW(final long imsi, final String date, final int tac, final long downDataVol,
            final long upDataVol, final long totalDataVol) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(IMSI, (imsi == IMSI_NULL ? "NULL" : imsi));
        valuesForTable.put(DATETIME_ID, date);
        valuesForTable.put(TAC, tac);
        valuesForTable.put(DOWNLOAD_DATA_VOLUME, (downDataVol == DATA_VOLUME_NULL ? "NULL" : downDataVol
                * KILOBYTE_TO_BYTE_RATE));
        valuesForTable.put(UPLOAD_DATA_VOLUME, (upDataVol == DATA_VOLUME_NULL ? "NULL" : upDataVol
                * KILOBYTE_TO_BYTE_RATE));
        valuesForTable.put(TOTAL_DATA_VOLUME, (totalDataVol == DATA_VOLUME_NULL ? "NULL" : totalDataVol
                * KILOBYTE_TO_BYTE_RATE));
        insertRow(TEMP_EVENT_E_GSM_PS_ALL_RAW, valuesForTable);
    }
}
