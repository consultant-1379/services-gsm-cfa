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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.AccessAreaDistributionSummaryService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.DistributionCellEventSummaryResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ewanggu
 * @since 2012
 * 
 */
public class AccessAreaDistributionSummaryAggTest extends BaseDataIntegrityTest<DistributionCellEventSummaryResult> {

    private static final String TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_ERR_15MIN = "#EVENT_E_GSM_CFA_HIER321_EVENTID_ERR_15MIN";

    private static final String TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_SUC_15MIN = "#EVENT_E_GSM_CFA_HIER321_EVENTID_SUC_15MIN";

    private AccessAreaDistributionSummaryService service;

    private String hashHier3Id = "";

    private String hashHier321Id1 = "";

    private String hashHier321Id2 = "";

    private String hashHier321IdIrrelevant1 = "";

    private String hashHier321IdIrrelevant2 = "";

    private String hashHier3IdIrrelevant1 = "";

    /**
     * 1. Create tables. 2. Insert test data to the tables.
     * 
     * @throws Exception
     */
    @Before
    public void onSetUp() throws Exception {
        createHashId();
        service = new AccessAreaDistributionSummaryService();
        attachDependencies(service);
        createTables();
        insertData();
        insertDataIntoTacGroupTable();
    }

    private void createHashId() {
        hashHier3Id = queryUtils.createHashIDForController(RAT_INTEGER_VALUE_FOR_2G, BSC1, ERICSSON) + "";
        hashHier321Id1 = queryUtils.createHashIDForCell(RAT_INTEGER_VALUE_FOR_2G, BSC1, "", GSMCELL1, ERICSSON) + "";
        hashHier321Id2 = queryUtils.createHashIDForCell(RAT_INTEGER_VALUE_FOR_2G, BSC1, "", GSMCELL2, ERICSSON) + "";
        hashHier3IdIrrelevant1 = queryUtils.createHashIDForController(RAT_INTEGER_VALUE_FOR_2G, BSC2, ERICSSON) + "";
        hashHier321IdIrrelevant1 = queryUtils.createHashIDForCell(RAT_INTEGER_VALUE_FOR_2G, BSC2, "", GSMCELL1,
                ERICSSON) + "";
        hashHier321IdIrrelevant2 = queryUtils.createHashIDForCell(RAT_INTEGER_VALUE_FOR_2G, BSC2, "", GSMCELL2,
                ERICSSON) + "";
    }

    @Test
    public void testOneDayQueryHashIdParam() throws Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        requestParameters.add(TIME_QUERY_PARAM, ONE_DAY);
        requestParameters.add(CONTROLLER_SQL_ID, hashHier3Id + "");
        requestParameters.add(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID);
        final String result = runQuery(service, requestParameters);
        verifyResult(result);
    }

    private void verifyResult(final String json) throws Exception {
        assertJSONSucceeds(json);
        final List<DistributionCellEventSummaryResult> results = getTranslator().translateResult(json,
                DistributionCellEventSummaryResult.class);

        assertThat(results.size(), is(2));
        validateAgainstGridDefinition(json, "GSM_CFA_CELL_SUMMARY_DISTRIBUTION");
        for (final DistributionCellEventSummaryResult result : results) {

            assertThat(result.getVendor(), is(ERICSSON));
            assertThat(result.getNumFailures(), is(3));
            assertThat(result.getNumImpactedSubscribers(), is(2));
            assertThat(result.getRatio(), is(50.00));
        }

        assertThat(results.get(0).getAccessArea(), is(GSMCELL2));
        assertThat(results.get(1).getAccessArea(), is(GSMCELL1));

    }

    private void createTables() throws Exception {
        createAndReplaceTable(TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_ERR_15MIN,
                "EVENT_E_GSM_CFA_HIER321_EVENTID_ERR_15MIN", HIER3_ID, HIER321_ID, CATEGORY_ID, DATETIME_ID,
                NO_OF_ERRORS);
        createAndReplaceTable(TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_SUC_15MIN,
                "EVENT_E_GSM_CFA_HIER321_EVENTID_SUC_15MIN", HIER3_ID, HIER321_ID, DATETIME_ID, NO_OF_SUCCESSES);
        createAndReplaceTable(TEMP_DIM_E_SGEH_HIER321, "DIM_E_SGEH_HIER321", HIERARCHY_3, HIERARCHY_1, HIER321_ID,
                HIER3_ID, VENDOR, RAT);
        createAndReplaceTable(TEMP_DIM_E_GSM_CFA_EVENTTYPE, "DIM_E_GSM_CFA_EVENTTYPE", CATEGORY_ID_DESC, CATEGORY_ID);
        createAndReplaceTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, "EVENT_E_GSM_CFA_ERR_RAW", HIER3_ID, HIER321_ID, IMSI, TAC,
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
        insertRowsInAgg(hashHier3Id, hashHier321Id1, dateTime, 3);
        insertRowsInAgg(hashHier3Id, hashHier321Id2, dateTime, 3);
        insertRowsInAgg(hashHier3IdIrrelevant1, hashHier321IdIrrelevant1, dateTime, 3);
        insertRowsInAgg(hashHier3IdIrrelevant1, hashHier321IdIrrelevant1, dateTime, 3);
    }

    private void populateHier321Table() throws SQLException {
        insertRowIntoHier321Table(RAT_FOR_GSM, BSC1, ERICSSON, hashHier321Id1, GSMCELL1, hashHier3Id);
        insertRowIntoHier321Table(RAT_FOR_GSM, BSC1, ERICSSON, hashHier321Id2, GSMCELL2, hashHier3Id);
        insertRowIntoHier321Table(RAT_FOR_GSM, BSC2, ERICSSON, hashHier321IdIrrelevant1, GSMCELL1,
                hashHier3IdIrrelevant1);
        insertRowIntoHier321Table(RAT_FOR_GSM, BSC2, ERICSSON, hashHier321IdIrrelevant2, GSMCELL2,
                hashHier3IdIrrelevant1);
    }

    private void insertRowIntoHier321Table(final int rat, final String controller, final String vendor,
            final String hier321Id, final String accessArea, final String hier3Id) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(RAT, rat);
        valuesForTable.put(HIERARCHY_3, controller);
        valuesForTable.put(HIERARCHY_1, accessArea);
        valuesForTable.put(VENDOR_PARAM_UPPER_CASE, vendor);
        valuesForTable.put(HIER321_ID, hier321Id);
        valuesForTable.put(HIER3_ID, hier3Id);
        insertRow(TEMP_DIM_E_SGEH_HIER321, valuesForTable);
    }

    private void insertRawTable() throws Exception {
        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(HIER321_ID, hashHier321Id1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(HIER3_ID, hashHier3Id);
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
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(HIER321_ID, hashHier321Id2);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(HIER3_ID, hashHier3Id);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(IMSI, 1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusHours(1));
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);

        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(HIER321_ID, hashHier321Id1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(HIER3_ID, hashHier3Id);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(IMSI, 2);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusHours(1));
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC);

        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.clear();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(HIER321_ID, hashHier321Id1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(HIER3_ID, hashHier3Id);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(IMSI, 2);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusHours(1));
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);

        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.clear();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(HIER321_ID, hashHier321Id2);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(HIER3_ID, hashHier3Id);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(IMSI, 2);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusHours(1));
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);

        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2 = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2.put(HIER321_ID, hashHier321IdIrrelevant1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2.put(HIER3_ID, hashHier3IdIrrelevant1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2.put(IMSI, 1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2.put(DATETIME_ID,
                DateTimeUtilities.getDateTimeMinusHours(1));
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2.put(CATEGORY_ID,
                GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2);

        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2.clear();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2.put(HIER321_ID, hashHier321IdIrrelevant2);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2.put(HIER3_ID, hashHier3IdIrrelevant1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2.put(IMSI, 1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2.put(DATETIME_ID,
                DateTimeUtilities.getDateTimeMinusHours(1));
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2.put(CATEGORY_ID,
                GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2);

    }

    private void insertRowsInAgg(final String hier3Id, final String hier321Id, final String datetime,
            final int instances) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(HIER321_ID, hier321Id);
        valuesForTable.put(HIER3_ID, hier3Id);
        valuesForTable.put(DATETIME_ID, datetime);
        valuesForTable.put(NO_OF_ERRORS, instances);
        valuesForTable.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_ERR_15MIN, valuesForTable);

        valuesForTable.remove(NO_OF_ERRORS);
        valuesForTable.remove(CATEGORY_ID);
        valuesForTable.put(NO_OF_SUCCESSES, instances);
        insertRow(TEMP_EVENT_E_GSM_CFA_HIER321_EVENTID_SUC_15MIN, valuesForTable);
    }

    private void insertEventTypeTable() throws Exception {
        final Map<String, Object> columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE = new HashMap<String, Object>();
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.put(CATEGORY_ID_DESC, "Call Drop");
        insertRow(TEMP_DIM_E_GSM_CFA_EVENTTYPE, columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE);
    }

    private void insertDataIntoTacGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }
}
