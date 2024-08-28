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
public class TerminalEventSummaryServiceRawTest extends BaseDataIntegrityTest<TerminalEventSummaryResult> {
    TerminalSummaryService TerminalSummaryService;

    private static final String DATETIME_ID_RAW = "2011-09-20 08:12:00";

    private static final String DATE_FROM_RAW = "20092011";

    private static final String DATE_TO_RAW = "20092011";

    private static final String TIME_FROM = "0900";

    private static final String TIME_TO = "0930";

    private static final String TEST_VALUE_TAC = "1090800";

    private static final String TEST_VALUE_IMSI = "11111119";

    /**
     * 1. Create tables.
     * 2. Insert test datas to the tables.
     * @throws Exception
     */
    @Before
    public void onSetUp() throws Exception {
        TerminalSummaryService = new TerminalSummaryService();
        attachDependencies(TerminalSummaryService);
        ReplaceTablesWithTempTablesTemplateUtils.addTableNameToReplace(DIM_E_SGEH_TAC, TEMP_DIM_E_SGEH_TAC);
        ReplaceTablesWithTempTablesTemplateUtils.addTableNameToReplace("DIM_E_GSM_CFA_EVENTTYPE",
                TEMP_DIM_E_GSM_CFA_EVENTTYPE);
        createTable();
        insertTopoData();
        insertRawData();
    }

    private void createTable() throws Exception {
        final Collection<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(IMSI);
        columnsForTable.add(TAC);
        columnsForTable.add(CATEGORY_ID);
        columnsForTable.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columnsForTable);

        columnsForTable.add(IMSI);
        columnsForTable.add(TAC);
        columnsForTable.add(CATEGORY_ID);
        columnsForTable.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_SUC_RAW, columnsForTable);

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
        valuesForTable.put(TAC, TEST_VALUE_EXCLUSIVE_TAC);
        valuesForTable.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP_NAME);
        insertRow(TEMP_GROUP_TYPE_E_TAC, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(TAC, TEST_VALUE_TAC);
        valuesForTable.put(VENDOR_NAME, "Sony Ericsson");
        valuesForTable.put(MARKETING_NAME, "TBD (AAB-1880030-BV)");
        insertRow(TEMP_DIM_E_SGEH_TAC, valuesForTable);
    }

    private void insertData(final String tac, final String categoryID, final String time, final int instances)
            throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        for (int i = 0; i < instances; i++) {
            valuesForTable.put(CATEGORY_ID, categoryID);
            valuesForTable.put(TAC, tac);
            valuesForTable.put(IMSI_PARAM, TEST_VALUE_IMSI);
            valuesForTable.put(DATETIME_ID, time);
            insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, valuesForTable);

            valuesForTable.put(CATEGORY_ID, categoryID);
            valuesForTable.put(TAC, tac);
            valuesForTable.put(IMSI_PARAM, TEST_VALUE_IMSI);
            valuesForTable.put(DATETIME_ID, time);
            insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, valuesForTable);
            valuesForTable.clear();
        }
    }

    private String getJsonResultCallDropAndCallSetup() throws URISyntaxException {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(DISPLAY_PARAM, GRID);
        requestParameters.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM_RAW);
        requestParameters.putSingle(DATE_TO_QUERY_PARAM, DATE_TO_RAW);
        requestParameters.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        requestParameters.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        requestParameters.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        requestParameters.putSingle(MAX_ROWS, DEFAULT_MAX_ROWS);
        requestParameters.putSingle(TAC, TEST_VALUE_TAC);
        return runQuery(TerminalSummaryService, requestParameters);
    }

    private void insertRawData() throws Exception {
        insertData(TEST_VALUE_TAC, GSM_CALL_DROP_CATEGORY_ID, DATETIME_ID_RAW, 2);
        insertData(TEST_VALUE_EXCLUSIVE_TAC, GSM_CALL_DROP_CATEGORY_ID, DATETIME_ID_RAW, 2);
        //Insert Call Setup Data
        insertData(TEST_VALUE_TAC, GSM_CALL_SETUP_FAILURE_CATEGORY_ID, DATETIME_ID_RAW, 2);
        insertData(TEST_VALUE_EXCLUSIVE_TAC, GSM_CALL_SETUP_FAILURE_CATEGORY_ID, DATETIME_ID_RAW, 2);

    }

    @Test
    public void testTerminalEventSummaryCallDropAndCallSetup() throws Exception {
        final String json = getJsonResultCallDropAndCallSetup();
        final ResultTranslator<TerminalEventSummaryResult> rt = getTranslator();
        final List<TerminalEventSummaryResult> eventResult = rt.translateResult(json, TerminalEventSummaryResult.class);
        assertThat(eventResult.size(), is(2));
        assertResult(eventResult);
    }

    private void assertResult(final List<TerminalEventSummaryResult> results) {
        for ( int i=0; i< results.size(); i++){
        	final TerminalEventSummaryResult rs = results.get(i);
        	if(i==0) {
        		assertEquals(rs.getEventType(),GSM_CALL_SETUP_FAILURES_CATEGORY_ID_DESC);
        	} else if( i== 1) {
        		assertEquals(rs.getEventType(),GSM_CALL_DROP_CATEGORY_ID_DESC);
        	}
            assertEquals(rs.getFailures(), 2);
        }
    }
}
