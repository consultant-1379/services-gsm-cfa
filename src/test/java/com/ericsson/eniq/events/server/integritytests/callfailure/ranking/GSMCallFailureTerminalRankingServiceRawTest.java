/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.callfailure.ranking;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.ranking.TerminalRankingService;
import com.ericsson.eniq.events.server.test.queryresults.ResultTranslator;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.GSMCallFailureTerminalRankingResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ejoegaf
 * @since 2011
 *
 */
public class GSMCallFailureTerminalRankingServiceRawTest extends
        BaseDataIntegrityTest<GSMCallFailureTerminalRankingResult> {

    private TerminalRankingService gsmCallFailureTerminalService;

    private static final int TAC1 = 100100;

    private static final int TAC2 = 100200;

    private static final int TAC3 = 100300;

    private static final String MANUFACTURER1 = "Mitsubishi";

    private static final String MANUFACTURER2 = "Siemens";

    private static final String MANUFACTURER3 = "Sony Ericsson";

    private static final String MODEL1 = "G410";

    private static final String MODEL2 = "A53";

    private static final String MODEL3 = "TBD (AAB-1880030-BV)";

    /**
     * 1. Create tables.
     * 2. Insert test data to the tables.
     * @throws Exception
     */
    @Before
    public void onSetUp() throws Exception {
        gsmCallFailureTerminalService = new TerminalRankingService();
        attachDependencies(gsmCallFailureTerminalService);
        createTables();

    }

    @Test
    public void testGetRankingData_Terminal_GSMCFA() throws Exception {

        final List<GSMCallFailureTerminalRankingResult> expectedResult = insertDataForSixEventsWithThreeTACsAndGetExpectedResults();
        insertDataThatShouldNotAffectResults();
        final List<GSMCallFailureTerminalRankingResult> actualResult = getData();
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void testGetRankingData_Terminal_GSMCFA_WithTACExclusion() throws Exception {

        final List<GSMCallFailureTerminalRankingResult> expectedResult = insertDataForSixEventsWithThreeTACsAndGetExpectedResults();
        insertDataThatShouldNotAffectResults();
        tweakExpectedResultForExclusiveTACs(expectedResult);
        insertDataIntoTacGroupTable();
        final List<GSMCallFailureTerminalRankingResult> actualResult = getData();
        assertTrue(expectedResult.equals(actualResult));
    }

    private void tweakExpectedResultForExclusiveTACs(final List<GSMCallFailureTerminalRankingResult> expectedResult) {
        //remove the events that contain TAC in the exclusive tac group
        expectedResult.remove(2);
    }

    private List<GSMCallFailureTerminalRankingResult> getData() throws Exception {

        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(DISPLAY_PARAM, GRID);
        requestParameters.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        requestParameters.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        requestParameters.putSingle(MAX_ROWS, DEFAULT_MAX_ROWS);

        final String json = runQuery(gsmCallFailureTerminalService, requestParameters);

        validateAgainstGridDefinition(json, "RAN_GSM_TAC_CFA");
        final ResultTranslator<GSMCallFailureTerminalRankingResult> rt = getTranslator();
        final List<GSMCallFailureTerminalRankingResult> actualResult = rt.translateResult(json,
                GSMCallFailureTerminalRankingResult.class);
        return actualResult;
    }

    private void insertDataIntoTacGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, TAC3);
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    /**
    * Create the prepare test tables for testing.
    * 
    * raw table: EVENT_E_GSM_CFA_ERR_RAW
    * @throws Exception
    */
    private void createTables() throws Exception {

        final Collection<String> columnsForRawTable = new ArrayList<String>();
        columnsForRawTable.add(TAC);
        columnsForRawTable.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columnsForRawTable);

        final Collection<String> columnsForDIMTable = new ArrayList<String>();
        columnsForDIMTable.add(VENDOR_NAME);
        columnsForDIMTable.add(MARKETING_NAME);
        columnsForDIMTable.add(TAC);
        createTemporaryTable(TEMP_DIM_E_SGEH_TAC, columnsForDIMTable);

    }

    private void populateDIMTACTable() throws SQLException {
        insertRowIntoDIMTACTable(MANUFACTURER1, MODEL1, TAC1);
        insertRowIntoDIMTACTable(MANUFACTURER2, MODEL2, TAC2);
        insertRowIntoDIMTACTable(MANUFACTURER3, MODEL3, TAC3);
    }

    private void insertRowIntoDIMTACTable(final String manufacturer, final String model, final int tac)
            throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(VENDOR_NAME, manufacturer);
        valuesForTable.put(MARKETING_NAME, model);
        valuesForTable.put(TAC, tac);
        insertRow(TEMP_DIM_E_SGEH_TAC, valuesForTable);
    }

    /**
     * This function is used to insert test data to the prepared tables.

     * @throws Exception
     */
    private List<GSMCallFailureTerminalRankingResult> insertDataForSixEventsWithThreeTACsAndGetExpectedResults()
            throws Exception {

        populateDIMTACTable();
        ReplaceTablesWithTempTablesTemplateUtils.addTableNameToReplace(DIM_E_SGEH_TAC, TEMP_DIM_E_SGEH_TAC);
        final String dateTime = DateTimeUtilities.getDateTimeMinus2Minutes();

        final List<GSMCallFailureTerminalRankingResult> expectedResultsList = new ArrayList<GSMCallFailureTerminalRankingResult>();
        insertRowsInRawAndPutExpectedResultInList(expectedResultsList, 1, MANUFACTURER1, MODEL1, dateTime, 3, TAC1);
        insertRowsInRawAndPutExpectedResultInList(expectedResultsList, 2, MANUFACTURER2, MODEL2, dateTime, 2, TAC2);
        insertRowsInRawAndPutExpectedResultInList(expectedResultsList, 3, MANUFACTURER3, MODEL3, dateTime, 1, TAC3);

        return expectedResultsList;
    }

    /**
     * This function is used to insert no. of event with IMSI to the table.
     * @param hier321Id         hash id.
     * @param datetime          The event time.
     * @param instances         The number of times to enter the row in the raw event table
     * @throws SQLException
     */
    private void insertRowsInRawAndPutExpectedResultInList(
            final List<GSMCallFailureTerminalRankingResult> expectedResultsList, final int expectedRank,
            final String manufacturer, final String model, final String datetime, final int instances, final int tac)
            throws SQLException {
        for (int i = 0; i < instances; i++) {
            insertRowInRawTable(datetime, tac);

        }
        expectedResultsList.add(getExpectedResult(expectedRank, manufacturer, model, tac, instances));
    }

    private void insertRowInRawTable(final String datetime, final int tac) throws SQLException {

        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(TAC, tac);
        valuesForTable.put(DATETIME_ID, datetime);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, valuesForTable);
    }

    private GSMCallFailureTerminalRankingResult getExpectedResult(final int rank, final String manufacturer,
            final String model, final int tac, final int numFailures) {
        final GSMCallFailureTerminalRankingResult expectedResult = new GSMCallFailureTerminalRankingResult();
        expectedResult.setRank(rank);
        expectedResult.setManufacturer(manufacturer);
        expectedResult.setModel(model);
        expectedResult.setTac(tac);
        expectedResult.setNumFailures(numFailures);
        return expectedResult;
    }

    private void insertDataThatShouldNotAffectResults() throws SQLException {
        final String oldDateTime = DateTimeUtilities.getDateTimeMinus25Minutes();
        //not in timerange of query
        insertRowInRawTable(oldDateTime, TAC1);
    }
}
