/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.callfailure.events;

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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.TerminalSummaryService;
import com.ericsson.eniq.events.server.test.queryresults.ResultTranslator;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.TerminalEventSummaryResult;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ewanggu
 * @since 2012
 *
 */
public class TerminalEventSummaryServiceAggTest extends BaseDataIntegrityTest<TerminalEventSummaryResult> {
    TerminalSummaryService terminalSummaryService;

    private static final String DATETIME_ID_DAY_1 = "2011-09-19 08:15:00";

    private static final String DATETIME_ID_DAY_2 = "2011-09-18 12:15:00";

    private static final String DATE_FROM_DAY = "13092011";

    private static final String DATE_TO_DAY = "20092011";

    private static final String DATETIME_ID_15MIN_1 = "2011-09-20 08:15:00";

    private static final String DATETIME_ID_15MIN_2 = "2011-09-20 12:15:00";

    private static final String DATE_FROM_15MIN = "20092011";

    private static final String DATE_TO_15MIN = "20092011";

    private static final String TIME_FROM = "0900";

    private static final String TIME_TO = "1530";

    private static final String TEST_VALUE_TAC = "1090801";

    private static final String TEST_VALUE_IMSI_1 = "11111119";

    private static final String TEST_VALUE_IMSI_2 = "11111118";

    private static final String TEMP_EVENT_E_GSM_CFA_TAC_EVENTID_ERR_15MIN = "#EVENT_E_GSM_CFA_TAC_EVENTID_ERR_15MIN";

    private static final String TEMP_EVENT_E_GSM_CFA_TAC_EVENTID_SUC_15MIN = "#EVENT_E_GSM_CFA_TAC_EVENTID_SUC_15MIN";

    private static final String TEMP_EVENT_E_GSM_CFA_TAC_EVENTID_ERR_DAY = "#EVENT_E_GSM_CFA_TAC_EVENTID_ERR_DAY";

    private static final String TEMP_EVENT_E_GSM_CFA_TAC_EVENTID_SUC_DAY = "#EVENT_E_GSM_CFA_TAC_EVENTID_SUC_DAY";

    /**
     * 1. Create tables.
     * 2. Insert test datas to the tables.
     * @throws Exception
     */
    @Before
    public void onSetUp() throws Exception {
        terminalSummaryService = new TerminalSummaryService();
        attachDependencies(terminalSummaryService);
        ReplaceTablesWithTempTablesTemplateUtils.addTableNameToReplace(DIM_E_SGEH_TAC, TEMP_DIM_E_SGEH_TAC);
        ReplaceTablesWithTempTablesTemplateUtils.addTableNameToReplace("DIM_E_GSM_CFA_EVENTTYPE",
                TEMP_DIM_E_GSM_CFA_EVENTTYPE);
        createTable();
        insertTopoData();
    }

    private void createTable() throws Exception {
        final Collection<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(IMSI);
        columnsForTable.add(TAC);
        columnsForTable.add(CATEGORY_ID);
        columnsForTable.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columnsForTable);

        columnsForTable.clear();
        columnsForTable.add(NO_OF_ERRORS);
        columnsForTable.add(TAC);
        columnsForTable.add(CATEGORY_ID);
        columnsForTable.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_TAC_EVENTID_ERR_15MIN, columnsForTable);

        columnsForTable.clear();
        columnsForTable.add(NO_OF_SUCCESSES);
        columnsForTable.add(TAC);
        columnsForTable.add(CATEGORY_ID);
        columnsForTable.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_TAC_EVENTID_SUC_15MIN, columnsForTable);

        columnsForTable.clear();
        columnsForTable.add(NO_OF_ERRORS);
        columnsForTable.add(TAC);
        columnsForTable.add(CATEGORY_ID);
        columnsForTable.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_TAC_EVENTID_ERR_DAY, columnsForTable);

        columnsForTable.clear();
        columnsForTable.add(NO_OF_SUCCESSES);
        columnsForTable.add(TAC);
        columnsForTable.add(CATEGORY_ID);
        columnsForTable.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_TAC_EVENTID_SUC_DAY, columnsForTable);

        columnsForTable.clear();
        columnsForTable.add(CATEGORY_ID);
        columnsForTable.add(CATEGORY_ID_DESC);
        createTemporaryTable(TEMP_DIM_E_GSM_CFA_EVENTTYPE, columnsForTable);

        columnsForTable.clear();
        columnsForTable.add(TAC);
        columnsForTable.add(VENDOR_NAME);
        columnsForTable.add(MARKETING_NAME);
        createTemporaryTable(TEMP_DIM_E_SGEH_TAC, columnsForTable);
    }

    private void insertTopoData() throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();

        valuesForTable.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID);
        valuesForTable.put(CATEGORY_ID_DESC, GSM_CALL_DROP_CATEGORY_ID_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_EVENTTYPE, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID);
        valuesForTable.put(CATEGORY_ID_DESC, GSM_CALL_SETUP_FAILURES_CATEGORY_ID_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_EVENTTYPE, valuesForTable);
        
        valuesForTable.clear();
        valuesForTable.put(TAC, Integer.valueOf(TEST_VALUE_EXCLUSIVE_TAC));
        valuesForTable.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP_NAME);
        insertRow(TEMP_GROUP_TYPE_E_TAC, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(TAC, TEST_VALUE_TAC);
        valuesForTable.put(VENDOR_NAME, "Sony Ericsson");
        valuesForTable.put(MARKETING_NAME, "TBD (AAB-1880030-BV)");
        insertRow(TEMP_DIM_E_SGEH_TAC, valuesForTable);
    }

    private void insertData(final String categoryID, final String tac, final String time, final int instances,
            final String imsi) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        for (int i = 0; i < instances; i++) {
            valuesForTable.put(CATEGORY_ID, categoryID);
            valuesForTable.put(TAC, tac);
            valuesForTable.put(IMSI_PARAM, imsi);
            valuesForTable.put(DATETIME_ID, time);
            insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, valuesForTable);
        }
    }

    private void insertAggData(final String aggTable, final String tac, final String categoryID, final String time,
            final int instances, final boolean issuc) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(CATEGORY_ID, categoryID);
        valuesForTable.put(TAC, tac);
        if (!issuc)
            valuesForTable.put(NO_OF_ERRORS, instances);
        else
            valuesForTable.put(NO_OF_SUCCESSES, instances);

        valuesForTable.put(DATETIME_ID, time);
        insertRow(aggTable, valuesForTable);
        valuesForTable.clear();
    }

    private void insertDataForAggTest(final String categoryID, final int instances, final String dateTimeID,
            final String aggTable, final boolean issuc) throws SQLException {
        insertData(categoryID, TEST_VALUE_TAC, dateTimeID, instances, TEST_VALUE_IMSI_1);
        insertData(categoryID, TEST_VALUE_EXCLUSIVE_TAC, dateTimeID, instances, TEST_VALUE_IMSI_1);
        insertData(categoryID, TEST_VALUE_TAC, dateTimeID, instances, TEST_VALUE_IMSI_2);
        insertData(categoryID, TEST_VALUE_EXCLUSIVE_TAC, dateTimeID, instances, TEST_VALUE_IMSI_2);
        insertAggData(aggTable, TEST_VALUE_TAC, categoryID, dateTimeID, instances, issuc);
    }

    private String getJsonResultAggCallDrop(final String dateFrom, final String dateTo) throws URISyntaxException {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(DISPLAY_PARAM, GRID);
        requestParameters.putSingle(DATE_FROM_QUERY_PARAM, dateFrom);
        requestParameters.putSingle(DATE_TO_QUERY_PARAM, dateTo);
        requestParameters.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        requestParameters.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        requestParameters.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        requestParameters.putSingle(MAX_ROWS, DEFAULT_MAX_ROWS);
        requestParameters.putSingle(TYPE_PARAM, TAC);
        requestParameters.putSingle(NODE_PARAM, "3333," + TEST_VALUE_TAC);
        return runQuery(terminalSummaryService, requestParameters);
    }

    private void insertAggData(final String aggTable, final String dateTimeID, final boolean issuc) throws Exception {
        insertDataForAggTest(GSM_CALL_DROP_CATEGORY_ID, 2, dateTimeID, aggTable, issuc);
        insertDataForAggTest(GSM_CALL_SETUP_FAILURE_CATEGORY_ID, 2, dateTimeID, aggTable, issuc);
    }

    @Test
    public void testTrackingAreaEventSummary15MinAggCallDrop() throws Exception {
        insertAggData(TEMP_EVENT_E_GSM_CFA_TAC_EVENTID_ERR_15MIN, DATETIME_ID_15MIN_1, false);
        insertAggData(TEMP_EVENT_E_GSM_CFA_TAC_EVENTID_SUC_15MIN, DATETIME_ID_15MIN_1, true);

        insertAggData(TEMP_EVENT_E_GSM_CFA_TAC_EVENTID_ERR_15MIN, DATETIME_ID_15MIN_2, false);
        insertAggData(TEMP_EVENT_E_GSM_CFA_TAC_EVENTID_SUC_15MIN, DATETIME_ID_15MIN_2, true);

        final String json = getJsonResultAggCallDrop(DATE_FROM_15MIN, DATE_TO_15MIN);

        final ResultTranslator<TerminalEventSummaryResult> rt = getTranslator();
        final List<TerminalEventSummaryResult> eventResult = rt.translateResult(json, TerminalEventSummaryResult.class);
        assertThat(eventResult.size(), is(2));
        assertResult(eventResult);
    }

    @Test
    public void testTrackingAreaEventSummaryDayAggCallDrop() throws Exception {
        insertAggData(TEMP_EVENT_E_GSM_CFA_TAC_EVENTID_ERR_DAY, DATETIME_ID_DAY_1, false);
        insertAggData(TEMP_EVENT_E_GSM_CFA_TAC_EVENTID_SUC_DAY, DATETIME_ID_DAY_1, true);

        insertAggData(TEMP_EVENT_E_GSM_CFA_TAC_EVENTID_ERR_DAY, DATETIME_ID_DAY_2, false);
        insertAggData(TEMP_EVENT_E_GSM_CFA_TAC_EVENTID_SUC_DAY, DATETIME_ID_DAY_2, true);

        final String json = getJsonResultAggCallDrop(DATE_FROM_DAY, DATE_TO_DAY);

        final ResultTranslator<TerminalEventSummaryResult> rt = getTranslator();
        final List<TerminalEventSummaryResult> eventResult = rt.translateResult(json, TerminalEventSummaryResult.class);
        assertThat(eventResult.size(), is(2));
        assertResult(eventResult);
    }

    private void assertResult(final List<TerminalEventSummaryResult> results) {
        for (int i=0; i< results.size(); i++) {
        	final TerminalEventSummaryResult rs = results.get(i);
        	if(i==0) {
        		assertEquals(rs.getEventType(),GSM_CALL_SETUP_FAILURES_CATEGORY_ID_DESC);
        	} else if( i== 1) {
        		assertEquals(rs.getEventType(),GSM_CALL_DROP_CATEGORY_ID_DESC);
        	}
            assertEquals(rs.getFailures(), 4);
            assertEquals(rs.getImpactedSubscribers(), 2);
        }
    }
}
