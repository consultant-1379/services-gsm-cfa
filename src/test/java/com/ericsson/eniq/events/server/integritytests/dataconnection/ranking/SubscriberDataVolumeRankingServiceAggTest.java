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
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_GSM_PS_ALL_IMSI_RANK_DAY;
import static org.junit.Assert.assertEquals;

public class SubscriberDataVolumeRankingServiceAggTest extends BaseDataIntegrityTest<SubscriberDataVolumeRankingResult> {

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
        insertData();
    }

    /*
     * The expected outcome is for IMSI_1 to rank first, then IMSI_3 and finally IMSI_2
     */
    @Test
    public void testSubscriberRankingByDataVolume() throws Exception {

        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(DISPLAY_PARAM, GRID_PARAM);
        requestParameters.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        requestParameters.putSingle(TZ_OFFSET, "+0100");
        requestParameters.putSingle(MAX_ROWS, "10");
        final String json = runQuery(subscriberDataVolumeRankingService, requestParameters);
        final ResultTranslator<SubscriberDataVolumeRankingResult> resultTranslator = getTranslator();
        final List<SubscriberDataVolumeRankingResult> rankingResult = resultTranslator.translateResult(json,
                SubscriberDataVolumeRankingResult.class);

        final List<SubscriberDataVolumeRankingResult> expectedResult = new ArrayList<SubscriberDataVolumeRankingResult>();
        expectedResult.add(new SubscriberDataVolumeRankingResult(1, TEST_IMSI_1, 15.0, 20.0, 35.0));
        expectedResult.add(new SubscriberDataVolumeRankingResult(2, TEST_IMSI_3, 9.0, 9.0, 18.0));
        expectedResult.add(new SubscriberDataVolumeRankingResult(3, TEST_IMSI_2, 10.0, 7.0, 17.0));

        assertEquals(expectedResult, rankingResult);
    }

    private void createTable() throws Exception {

        final Map<String, Nullable> columnsForEventsTable = new HashMap<String, Nullable>();
        columnsForEventsTable.put(IMSI, Nullable.CAN_BE_NULL);
        columnsForEventsTable.put(DOWNLOAD_DATA_VOLUME, Nullable.CAN_BE_NULL);
        columnsForEventsTable.put(UPLOAD_DATA_VOLUME, Nullable.CAN_BE_NULL);
        columnsForEventsTable.put(TOTAL_DATA_VOLUME, Nullable.CAN_BE_NULL);
        columnsForEventsTable.put(DATETIME_ID, Nullable.CANNOT_BE_NULL);
        createTemporaryTable(TEMP_EVENT_E_GSM_PS_ALL_IMSI_RANK_DAY, columnsForEventsTable);

    }

    private void insertData() throws Exception {

        final String dateTime = DateTimeUtilities.getDateMinus48Hours();

        insertRowToPSAggTable(TEST_IMSI_0, dateTime, 10, 7, 17);

        insertRowToPSAggTable(TEST_IMSI_0, dateTime, DATA_VOLUME_NULL, DATA_VOLUME_NULL, DATA_VOLUME_NULL);

        insertRowToPSAggTable(TEST_IMSI_1, dateTime, 5, 10, 15);

        insertRowToPSAggTable(TEST_IMSI_2, dateTime, 10, 7, 17);

        insertRowToPSAggTable(TEST_IMSI_2, dateTime, DATA_VOLUME_NULL, DATA_VOLUME_NULL, DATA_VOLUME_NULL);

        insertRowToPSAggTable(TEST_IMSI_3, dateTime, 9, 9, 18);

        insertRowToPSAggTable(TEST_IMSI_1, dateTime, 10, 10, 20);

        insertRowToPSAggTable(IMSI_NULL, dateTime, 100, 100, 200);

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
    private void insertRowToPSAggTable(final long imsi, final String date, final long downDataVol,
            final long upDataVol, final long totalDataVol) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(IMSI, (imsi == IMSI_NULL ? "NULL" : imsi));
        valuesForTable.put(DATETIME_ID, date);
        valuesForTable.put(DOWNLOAD_DATA_VOLUME, (downDataVol == DATA_VOLUME_NULL ? "NULL" : downDataVol
                * KILOBYTE_TO_BYTE_RATE));
        valuesForTable.put(UPLOAD_DATA_VOLUME, (upDataVol == DATA_VOLUME_NULL ? "NULL" : upDataVol
                * KILOBYTE_TO_BYTE_RATE));
        valuesForTable.put(TOTAL_DATA_VOLUME, (totalDataVol == DATA_VOLUME_NULL ? "NULL" : totalDataVol
                * KILOBYTE_TO_BYTE_RATE));
        insertRow(TEMP_EVENT_E_GSM_PS_ALL_IMSI_RANK_DAY, valuesForTable);
    }
}
