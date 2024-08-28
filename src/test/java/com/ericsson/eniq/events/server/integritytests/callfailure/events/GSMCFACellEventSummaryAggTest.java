/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.callfailure.events;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.AccessAreaSummaryService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.GSMAccessAreaCallFailureEventSummaryResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ehorpte
 * @since 2011
 * 
 */
public class GSMCFACellEventSummaryAggTest extends BaseDataIntegrityTest<GSMAccessAreaCallFailureEventSummaryResult> {

    private static final String TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_ERR_15MIN = "#EVENT_E_GSM_CFA_HIER321_EVENTID_ERR_15MIN";

    private static final String TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_SUC_15MIN = "#EVENT_E_GSM_CFA_HIER321_EVENTID_SUC_15MIN";

    private AccessAreaSummaryService gsmCFAAccessAreaEventSummaryService;

    private long hashHier321Id;

    private long hashHier321IdIrrelevant1;

    private long hashHier321IdIrrelevant2;

    private long hashHier321IdIrrelevant3;

    private long hashHier321IdIrrelevant4;

    /**
     * 1. Create tables. 2. Insert test data to the tables.
     * 
     * @throws Exception
     */
    @Before
    public void onSetUp() throws Exception {
        createHashId();
        gsmCFAAccessAreaEventSummaryService = new AccessAreaSummaryService();
        attachDependencies(gsmCFAAccessAreaEventSummaryService);
        createTables();
        insertData();
        insertDataIntoTacGroupTable();
    }

    private void createHashId() {
        hashHier321Id = queryUtils.createHashIDForCell(RAT_INTEGER_VALUE_FOR_2G, BSC1, "", GSMCELL1, ERICSSON);
        hashHier321IdIrrelevant1 = queryUtils.createHashIDForCell(RAT_INTEGER_VALUE_FOR_2G, BSC2, "", GSMCELL1,
                ERICSSON);
        hashHier321IdIrrelevant2 = queryUtils.createHashIDForCell(RAT_INTEGER_VALUE_FOR_2G, BSC1, "", GSMCELL2,
                ERICSSON);
        hashHier321IdIrrelevant3 = queryUtils.createHashIDForCell(RAT_INTEGER_VALUE_FOR_2G, BSC1, "", GSMCELL1,
                SONY_ERICSSON);
        hashHier321IdIrrelevant4 = queryUtils.createHashIDForCell(RAT_INTEGER_VALUE_FOR_3G, BSC1, "", GSMCELL1,
                ERICSSON);
    }

    @Test
    public void testOneDayQueryNodeParam() throws Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        requestParameters.add(TIME_QUERY_PARAM, ONE_DAY);
        requestParameters.add(NODE_PARAM, GSMCELL1 + ",," + BSC1 + "," + ERICSSON + "," + "GSM");
        requestParameters.add(TYPE_PARAM, TYPE_CELL);
        final String result = runQuery(gsmCFAAccessAreaEventSummaryService, requestParameters);
        verifyResult(result);
    }

    @Test
    public void testOneDayQueryHashIdParam() throws Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        requestParameters.add(TIME_QUERY_PARAM, ONE_DAY);
        requestParameters.add(CELL_SQL_ID, Long.toString(hashHier321Id));
        final String result = runQuery(gsmCFAAccessAreaEventSummaryService, requestParameters);
        verifyResult(result);
    }

    private void verifyResult(final String json) throws Exception {
        assertJSONSucceeds(json);
        final List<GSMAccessAreaCallFailureEventSummaryResult> results = getTranslator().translateResult(json,
                GSMAccessAreaCallFailureEventSummaryResult.class);

        assertThat(results.size(), is(2));
        validateAgainstGridDefinition(json, "NETWORK_GSM_EVENT_ANALYSIS_SUMMARY_CELL");
        GSMAccessAreaCallFailureEventSummaryResult result = results.get(0);

        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(BSC1));
        assertThat(result.getEventDescription(), is("Call Setup Failures"));
        assertThat(result.getNumFailures(), is(3));
        assertThat(result.getNumImpactedSubscribers(), is(1));
        assertThat(result.getHier321Id(), is(hashHier321Id));
        assertThat(result.getAccessArea(), is(GSMCELL1));
        assertThat(result.getRatio(), is(25.0));

        result = results.get(1);

        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(BSC1));
        assertThat(result.getNumFailures(), is(3));
        assertThat(result.getNumImpactedSubscribers(), is(1));
        assertThat(result.getHier321Id(), is(hashHier321Id));
        assertThat(result.getAccessArea(), is(GSMCELL1));
        assertThat(result.getRatio(), is(25.0));
    }

    private void createTables() throws Exception {
        createAndReplaceTable(TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_ERR_15MIN,
                "EVENT_E_GSM_CFA_HIER321_EVENTID_ERR_15MIN", HIER321_ID, EVENT_ID, CATEGORY_ID, DATETIME_ID,
                NO_OF_ERRORS, NO_OF_TOTAL_ERR_SUBSCRIBERS);
        createAndReplaceTable(TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_SUC_15MIN,
                "EVENT_E_GSM_CFA_HIER321_EVENTID_SUC_15MIN", HIER321_ID, DATETIME_ID, NO_OF_SUCCESSES);
        createAndReplaceTable(TEMP_DIM_E_SGEH_HIER321, "DIM_E_SGEH_HIER321", HIERARCHY_3, HIERARCHY_1, HIER321_ID,
                VENDOR, RAT);
        createAndReplaceTable(TEMP_DIM_E_GSM_CFA_EVENTTYPE, "DIM_E_GSM_CFA_EVENTTYPE", EVENT_ID_DESC, EVENT_ID,
                CATEGORY_ID_DESC, CATEGORY_ID);
        createAndReplaceTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, "EVENT_E_GSM_CFA_ERR_RAW", HIER321_ID, EVENT_ID, IMSI, TAC,
                DATETIME_ID, CATEGORY_ID);
    }

    private void createAndReplaceTable(final String tempTableName, final String tableNameToReplace,
            final String... columns) throws Exception {
        final Collection<String> columnsForTable = new ArrayList<String>();
        for (final String column : columns) {
            columnsForTable.add(column);
        }
        createTemporaryTable(tempTableName, columnsForTable);
        ReplaceTablesWithTempTablesTemplateUtils.addTableNameToReplace(tableNameToReplace, tempTableName);
    }

    private void insertData() throws Exception {
        populateHier321Table();
        insertRawTable();
        insertEventTypeTable();
        final String dateTime = DateTimeUtilities.getDateTimeMinusHours(1);
        insertRowsInAgg(hashHier321Id, dateTime, 3);
    }

    private void populateHier321Table() throws SQLException {
        insertRowIntoHier321Table(RAT_FOR_GSM, BSC1, ERICSSON, hashHier321Id, GSMCELL1);
        insertRowIntoHier321Table(RAT_FOR_GSM, BSC2, ERICSSON, hashHier321IdIrrelevant1, GSMCELL1);
        insertRowIntoHier321Table(RAT_FOR_GSM, BSC1, ERICSSON, hashHier321IdIrrelevant2, GSMCELL2);
        insertRowIntoHier321Table(RAT_FOR_GSM, BSC1, ERICSSON, hashHier321IdIrrelevant3, GSMCELL1);
        insertRowIntoHier321Table(RAT_FOR_GSM, BSC1, ERICSSON, hashHier321IdIrrelevant4, GSMCELL1);
    }

    private void insertRowIntoHier321Table(final int rat, final String controller, final String vendor,
            final long hier321Id, final String accessArea) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(RAT, rat);
        valuesForTable.put(HIERARCHY_3, controller);
        valuesForTable.put(HIERARCHY_1, accessArea);
        valuesForTable.put(VENDOR_PARAM_UPPER_CASE, vendor);
        valuesForTable.put(HIER321_ID, hier321Id);
        insertRow(TEMP_DIM_E_SGEH_HIER321, valuesForTable);
    }

    private void insertRawTable() throws Exception {
        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(HIER321_ID, hashHier321Id);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(EVENT_ID, GSM_CALL_DROP_EVENT_ID_AS_INTEGER);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(IMSI, 1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusHours(1));
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusHours(1));
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusHours(1));
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);

        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.clear();

        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(HIER321_ID, hashHier321Id);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(EVENT_ID, GSM_CALL_DROP_EVENT_ID_AS_INTEGER);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(IMSI, 1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusHours(1));
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusHours(1));
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusHours(1));
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);

        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(HIER321_ID, hashHier321Id);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(EVENT_ID, GSM_CALL_DROP_EVENT_ID_AS_INTEGER);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(IMSI, 2);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusHours(1));
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC);

        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1 = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1.put(HIER321_ID, hashHier321IdIrrelevant1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1.put(EVENT_ID, GSM_CALL_DROP_EVENT_ID_AS_INTEGER);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1.put(IMSI, 1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1.put(DATETIME_ID,
                DateTimeUtilities.getDateTimeMinusHours(1));
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1.put(CATEGORY_ID,
                GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1);

        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2 = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2.put(HIER321_ID, hashHier321IdIrrelevant2);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2.put(EVENT_ID, GSM_CALL_DROP_EVENT_ID_AS_INTEGER);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2.put(IMSI, 1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2.put(DATETIME_ID,
                DateTimeUtilities.getDateTimeMinusHours(1));
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2.put(CATEGORY_ID,
                GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2);

        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_3 = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_3.put(HIER321_ID, hashHier321IdIrrelevant3);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_3.put(EVENT_ID, GSM_CALL_DROP_EVENT_ID_AS_INTEGER);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_3.put(IMSI, 1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_3.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_3.put(DATETIME_ID,
                DateTimeUtilities.getDateTimeMinusHours(1));
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_3.put(CATEGORY_ID,
                GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_3);

        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_4 = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_4.put(HIER321_ID, hashHier321IdIrrelevant4);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_4.put(EVENT_ID, GSM_CALL_DROP_EVENT_ID_AS_INTEGER);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_4.put(IMSI, 1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_4.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_4.put(DATETIME_ID,
                DateTimeUtilities.getDateTimeMinusHours(1));
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_4.put(CATEGORY_ID,
                GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_4);

        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_DATETIME_ID = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_DATETIME_ID.put(HIER321_ID, hashHier321IdIrrelevant4);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_DATETIME_ID.put(EVENT_ID, GSM_CALL_DROP_EVENT_ID_AS_INTEGER);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_DATETIME_ID.put(IMSI, 1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_DATETIME_ID.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_DATETIME_ID.put(DATETIME_ID,
                DateTimeUtilities.getDateTimeMinusHours(2));
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_DATETIME_ID.put(CATEGORY_ID,
                GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_DATETIME_ID);
    }

    private void insertRowsInAgg(final long hier321Id, final String datetime, final int instances) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(HIER321_ID, hier321Id);
        valuesForTable.put(DATETIME_ID, datetime);
        valuesForTable.put(NO_OF_ERRORS, instances);
        valuesForTable.put(NO_OF_TOTAL_ERR_SUBSCRIBERS, 1);
        valuesForTable.put(EVENT_ID, GSM_CALL_DROP_EVENT_ID_AS_INTEGER);
        valuesForTable.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_ERR_15MIN, valuesForTable);
        valuesForTable.remove(NO_OF_TOTAL_ERR_SUBSCRIBERS);
        valuesForTable.remove(CATEGORY_ID);
        valuesForTable.remove(NO_OF_ERRORS);
        valuesForTable.remove(EVENT_ID);
        valuesForTable.put(NO_OF_SUCCESSES, instances);
        insertRow(TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_SUC_15MIN, valuesForTable);

        valuesForTable.clear();

        valuesForTable.put(HIER321_ID, hier321Id);
        valuesForTable.put(DATETIME_ID, datetime);
        valuesForTable.put(NO_OF_ERRORS, instances);
        valuesForTable.put(NO_OF_TOTAL_ERR_SUBSCRIBERS, 1);
        valuesForTable.put(EVENT_ID, GSM_CALL_DROP_EVENT_ID_AS_INTEGER);
        valuesForTable.put(CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_ERR_15MIN, valuesForTable);
        valuesForTable.remove(NO_OF_TOTAL_ERR_SUBSCRIBERS);
        valuesForTable.remove(CATEGORY_ID);
        valuesForTable.remove(NO_OF_ERRORS);
        valuesForTable.remove(EVENT_ID);
        valuesForTable.put(NO_OF_SUCCESSES, instances);
        insertRow(TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_SUC_15MIN, valuesForTable);

    }

    private void insertEventTypeTable() throws Exception {
        final Map<String, Object> columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE = new HashMap<String, Object>();
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.put(EVENT_ID, GSM_CALL_DROP_EVENT_ID_AS_INTEGER);
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.put(EVENT_ID_DESC, "CS Call Release");
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.put(CATEGORY_ID_DESC, "Call Drop");
        insertRow(TEMP_DIM_E_GSM_CFA_EVENTTYPE, columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE);

        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.clear();

        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.put(EVENT_ID, GSM_CALL_DROP_EVENT_ID_AS_INTEGER);
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.put(EVENT_ID_DESC, "CS Call Release");
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.put(CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER);
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.put(CATEGORY_ID_DESC, "Call Setup Failures");
        insertRow(TEMP_DIM_E_GSM_CFA_EVENTTYPE, columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE);

    }

    private void insertDataIntoTacGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }
}
