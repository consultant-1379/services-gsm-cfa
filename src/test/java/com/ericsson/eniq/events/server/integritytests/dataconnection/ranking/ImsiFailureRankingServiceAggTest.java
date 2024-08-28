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
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_GSM_PS_ALL_IMSI_RANK_DAY;
import static org.junit.Assert.assertTrue;

/**
 * 
 * @author eramiye
 * @since 2012
 *
 */
public class ImsiFailureRankingServiceAggTest extends BaseDataIntegrityTest<ImsiFailureRankingResult> {

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
        tweakExpectedResultToRemove2minEntry(expectedResult);
        final List<ImsiFailureRankingResult> actualResult = getQueryData();
        assertTrue(expectedResult.equals(actualResult));
    }

    private void tweakExpectedResultToRemove2minEntry(final List<ImsiFailureRankingResult> expectedResult) {
        // remove the events that contain 2 min entry in the expected result.
        expectedResult.remove(2);
    }

    private List<ImsiFailureRankingResult> getQueryData() throws Exception {

        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(DISPLAY_PARAM, GRID);
        requestParameters.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        requestParameters.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        requestParameters.putSingle(MAX_ROWS, DEFAULT_MAX_ROWS);

        final String json = runQuery(connectionFailureByImsiRankingService, requestParameters);

        final ResultTranslator<ImsiFailureRankingResult> rt = getTranslator();
        final List<ImsiFailureRankingResult> actualResult = rt.translateResult(json, ImsiFailureRankingResult.class);
        return actualResult;
    }

    /**
     * Create the prepare test tables for testing.
     * 
     * agg table: TEMP_EVENT_E_GSM_PS_ALL_IMSI_RANK_DAY
     * 
     * @throws Exception
     */
    private void createTables() throws Exception {
        final Collection<String> columnsForAggTable = new ArrayList<String>();
        columnsForAggTable.add(IMSI);
        columnsForAggTable.add(DATETIME_ID);
        columnsForAggTable.add(NO_OF_ERRORS);
        createTemporaryTable(TEMP_EVENT_E_GSM_PS_ALL_IMSI_RANK_DAY, columnsForAggTable);

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
        insertRowsInRawAndPutExpectedResultInList(expectedResultsList, 1, "460000748502770", dateTime48hrs, 18);
        insertRowsInRawAndPutExpectedResultInList(expectedResultsList, 2, "650294940290178", dateTime48hrs, 17);
        insertRowsInRawAndPutExpectedResultInList(expectedResultsList, 3, "357180042358664", dateTime2mins, 16);

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
            final int expectedRank, final String imsi, final String datetime, final int noe) throws SQLException {

        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(IMSI, imsi);
        valuesForTable.put(DATETIME_ID, datetime);
        valuesForTable.put(NO_OF_ERRORS, noe);

        insertRow(TEMP_EVENT_E_GSM_PS_ALL_IMSI_RANK_DAY, valuesForTable);
        expectedResultsList.add(getExpectedResult(expectedRank, imsi, noe));
    }

    private ImsiFailureRankingResult getExpectedResult(final int rank, final String imsi, final int numFailures) {
        final ImsiFailureRankingResult expectedResult = new ImsiFailureRankingResult();
        expectedResult.setRank(rank);
        expectedResult.setImsi(imsi);
        expectedResult.setNumFailures(numFailures);
        return expectedResult;
    }

}
