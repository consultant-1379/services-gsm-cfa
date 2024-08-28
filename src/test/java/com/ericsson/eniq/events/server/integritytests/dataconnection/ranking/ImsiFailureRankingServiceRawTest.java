/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.dataconnection.ranking;

import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.dataconnection.ranking.SubscriberFailureRankingService;
import com.ericsson.eniq.events.server.test.queryresults.ResultTranslator;
import com.ericsson.eniq.events.server.test.queryresults.gsm.dataconnection.ImsiFailureRankingResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.MultivaluedMap;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_GSM_PS_ALL_RAW;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_GROUP_TYPE_E_TAC;
import static org.junit.Assert.assertTrue;

/**
 * 
 * @author eramiye
 * @since 2012
 *
 */
public class ImsiFailureRankingServiceRawTest extends BaseDataIntegrityTest<ImsiFailureRankingResult> {

    private SubscriberFailureRankingService connectionFailureByImsiRankingService;

    /**
     * 1. Create tables. 2. Insert test data to the tables.
     * 
     * @throws Exception
     */
    @Before
    public void onSetUp() throws Exception {
        connectionFailureByImsiRankingService = new SubscriberFailureRankingService();
        attachDependencies(connectionFailureByImsiRankingService);
        createTables();

    }

    @Test
    public void testConnectionFailureByImsiRankingData() throws Exception {

        final List<ImsiFailureRankingResult> expectedResult = insertImsiData();
        tweakExpectedResultToRemoveAggrEntries(expectedResult);
        final List<ImsiFailureRankingResult> actualResult = getDataForFiveMinuteQuery();
        assertTrue(expectedResult.equals(actualResult));
    }

    @Test
    public void testTACExclusion() throws Exception {

        final List<ImsiFailureRankingResult> expectedResult = insertImsiData();
        tweakExpectedResultForExclusiveTACs(expectedResult);
        tweakExpectedResultToRemoveAggrEntriesForTac(expectedResult);
        insertDataIntoTacGroupTable();
        final List<ImsiFailureRankingResult> actualResult = getDataForFiveMinuteQuery();
        assertTrue(expectedResult.equals(actualResult));
    }

    private void tweakExpectedResultForExclusiveTACs(final List<ImsiFailureRankingResult> expectedResult) {
        // remove the events that contain TAC in the exclusive tac group
        expectedResult.remove(0);
        // promote the rank
        expectedResult.get(0).setRank(1);
        expectedResult.get(1).setRank(2);
    }

    private void tweakExpectedResultToRemoveAggrEntriesForTac(final List<ImsiFailureRankingResult> expectedResult) {
        // remove the events that contain TAC in the exclusive tac group
        expectedResult.remove(1);
    }

    private void tweakExpectedResultToRemoveAggrEntries(final List<ImsiFailureRankingResult> expectedResult) {
        // remove the events that contain TAC in the exclusive tac group
        expectedResult.remove(2);
        expectedResult.remove(0);
        //promote the rank
        expectedResult.get(0).setRank(1);
    }

    private List<ImsiFailureRankingResult> getDataForFiveMinuteQuery() throws Exception {

        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(DISPLAY_PARAM, GRID);
        requestParameters.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        requestParameters.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        requestParameters.putSingle(MAX_ROWS, DEFAULT_MAX_ROWS);

        final String json = runQuery(connectionFailureByImsiRankingService, requestParameters);

        final ResultTranslator<ImsiFailureRankingResult> rt = getTranslator();
        final List<ImsiFailureRankingResult> actualResult = rt.translateResult(json, ImsiFailureRankingResult.class);
        return actualResult;
    }

    private void insertDataIntoTacGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    /**
     * Create the prepare test tables for testing.
     * 
     * raw table: EVENT_E_GSM_PS_ERR_RAW
     * 
     * @throws Exception
     */
    private void createTables() throws Exception {
        final Collection<String> columnsForRawTable = new ArrayList<String>();
        columnsForRawTable.add(IMSI);
        columnsForRawTable.add(DATETIME_ID);
        columnsForRawTable.add(TAC);
        columnsForRawTable.add(NO_OF_ERRORS);
        createTemporaryTable(TEMP_EVENT_E_GSM_PS_ALL_RAW, columnsForRawTable);

    }

    /**
     * This function is used to insert test data to the prepared tables.
     * 
     * @throws Exception
     */
    private List<ImsiFailureRankingResult> insertImsiData() throws Exception {

        final String dateTime48hrs = DateTimeUtilities.getDateMinus48Hours();
        final String dateTime2mins = DateTimeUtilities.getDateTimeMinus2Minutes();

        final List<ImsiFailureRankingResult> expectedResultsList = new ArrayList<ImsiFailureRankingResult>();
        insertRowsInRawAndPutExpectedResultInList(expectedResultsList, 1, "0", dateTime2mins, 1, 18,
                SAMPLE_EXCLUSIVE_TAC);
        insertRowsInRawAndPutExpectedResultInList(expectedResultsList, 1, "460000748502770", dateTime2mins, 1, 18,
                SAMPLE_EXCLUSIVE_TAC);
        insertRowsInRawAndPutExpectedResultInList(expectedResultsList, 2, "650294940290178", dateTime2mins, 1, 17,
                SAMPLE_TAC);
        insertRowsInRawAndPutExpectedResultInList(expectedResultsList, 3, "357180042358664", dateTime48hrs, 1, 16,
                SAMPLE_TAC);

        //Test for IMSI = 0 (It should be excluded from the ranking).
        insertRowsInRawAndPutExpectedResultInList(null, 1, "0", dateTime48hrs, 1, 20, SAMPLE_TAC);

        return expectedResultsList;
    }

    /**
     * This function is used to insert no. of event with IMSI to the table.
     * 
     * @param imsi
     *          imsi.
     * @param datetime
     *          The event time.
     * @param instances
     *          The number of times to enter the row in the raw event table
     * @throws SQLException
     */
    private void insertRowsInRawAndPutExpectedResultInList(final List<ImsiFailureRankingResult> expectedResultsList,
            final int expectedRank, final String imsi, final String datetime, final int instances, final int noe,
            final int tac) throws SQLException {
        for (int i = 0; i < instances; i++) {
            final Map<String, Object> valuesForTable = new HashMap<String, Object>();
            valuesForTable.put(IMSI, imsi);
            valuesForTable.put(DATETIME_ID, datetime);
            valuesForTable.put(TAC, tac);
            valuesForTable.put(NO_OF_ERRORS, noe);
            insertRow(TEMP_EVENT_E_GSM_PS_ALL_RAW, valuesForTable);
        }
        if(expectedResultsList != null){
            if(!imsi.equalsIgnoreCase("0"))   {
                expectedResultsList.add(getExpectedResult(expectedRank, imsi, noe));
            }
        }
    }

    private ImsiFailureRankingResult getExpectedResult(final int rank, final String imsi, final int numFailures) {
        final ImsiFailureRankingResult expectedResult = new ImsiFailureRankingResult();
        expectedResult.setRank(rank);
        expectedResult.setImsi(imsi);
        expectedResult.setNumFailures(numFailures);
        return expectedResult;
    }
}
