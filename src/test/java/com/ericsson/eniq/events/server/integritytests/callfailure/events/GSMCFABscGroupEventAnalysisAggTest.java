/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.ControllerGroupEventAnalysisService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.GSMBscEventAnalysisResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ETHOMIT
 * @since 2012
 *
 */
public class GSMCFABscGroupEventAnalysisAggTest extends BaseDataIntegrityTest<GSMBscEventAnalysisResult> {

    private static final String TEST_VALUE_CATEGORY_ID = GSM_CALL_DROP_CATEGORY_ID;

    private static final String TEST_VALUE_HIER3_ID_1 = "332349676";

    private static final String TEST_VALUE_HIER3_ID_2 = "735598338";

    private static final String TEST_VALUE_HIER3_ID_3 = "804123789";

    private static final String TEST_VALUE_HIER321_ID_1 = "999999804123789";

    private static final String TEST_VALUE_HIER321_ID_2 = "111111804123789";

    private static final String TEST_VALUE_HIER321_ID_3 = "222222804123789";

    private String dateTime;

    private static final String TEST_VALUE_CATEGORY_ID_DESC = "Call Drops";

    private static final String TEST_VALUE_IMSI = "46000608201336";

    private static final String TEST_VALUE_IMSI_1 = "46000936201216";

    private static final int TEST_VALUE_TAC = 100100;

    private static final String TEST_VALUE_VENDOR = "Ericsson";

    private static final String TEST_VALUE_BSC_CONTROL_GROUP = "Control_Group";

    private static final String TEMP_EVENT_E_GSM_CFA_HIER3_EVENTID_ERR_15MIN = "#EVENT_E_GSM_CFA_HIER3_EVENTID_ERR_15MIN";
    private static final String TEMP_EVENT_E_GSM_CFA_HIER3_EVENTID_SUC_15MIN = "#EVENT_E_GSM_CFA_HIER3_EVENTID_SUC_15MIN";

    private long hashId;

    private ControllerGroupEventAnalysisService service;

    @Before
    public void onSetUp() throws Exception {
        service = new ControllerGroupEventAnalysisService();
        attachDependencies(service);
        createHashId();
        createLookupTables();
        createEventTable();
        insertLookupData();
        insertDataIntoTacGroupTable();
        insertRawEventData();
        insertAggregatedEventData();
    }

    private void createHashId() {
        hashId = queryUtils.createHashIDForController("0", TEST_VALUE_GSM_CONTROLLER1_NAME, TEST_VALUE_VENDOR);
        System.out.println(hashId);
    }

    @Test
    public void testOneDayQuery() throws URISyntaxException, Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TIME_QUERY_PARAM, ONE_DAY);
        requestParameters.add(TYPE_PARAM, BSC);
        requestParameters.add(GROUP_NAME_PARAM, TEST_VALUE_BSC_GROUP);
        requestParameters.add(CATEGORY_ID, TEST_VALUE_CATEGORY_ID);
        final String result = runQuery(service, requestParameters);
        verifyResult(result);
    }

    private void verifyResult(final String json) throws Exception {
        assertJSONSucceeds(json);

        final List<GSMBscEventAnalysisResult> results = getTranslator().translateResult(json,
                GSMBscEventAnalysisResult.class);

        assertThat(results.size(), is(2));
        final GSMBscEventAnalysisResult firstResult = results.get(0);
        assertThat(firstResult.getCategoryDescription(), is(TEST_VALUE_CATEGORY_ID_DESC));
        assertThat(firstResult.getNumberOfFailures(), is(15));
        assertThat(firstResult.getNumberOfImpactedSubscribers(), is(1));
        assertThat(firstResult.getNumberofImpectedCells(), is(2));
        assertThat(firstResult.getHier3_ID(), is(TEST_VALUE_HIER3_ID_1));

        final GSMBscEventAnalysisResult secondResult = results.get(1);
        assertThat(secondResult.getCategoryDescription(), is(TEST_VALUE_CATEGORY_ID_DESC));
        assertThat(secondResult.getNumberOfFailures(), is(7));
        assertThat(secondResult.getNumberOfImpactedSubscribers(), is(2));
        assertThat(secondResult.getNumberofImpectedCells(), is(2));
        assertThat(secondResult.getHier3_ID(), is(TEST_VALUE_HIER3_ID_2));

    }

    private void insertRawEventData() throws Exception {
        dateTime = DateTimeUtilities.getDateTimeMinusMinutes(25 + GSM_CFA_LATENCY_ON_ONE_DAY_QUERY);
        insertData(TEST_VALUE_IMSI, TEST_VALUE_CATEGORY_ID, TEST_VALUE_HIER3_ID_1, TEST_VALUE_HIER321_ID_1,
                TEST_VALUE_TAC, dateTime, 3); //1 impacted subscribers for HIER3_ID_1 as it is 3 instances for same imsi
        insertData(TEST_VALUE_IMSI, TEST_VALUE_CATEGORY_ID, TEST_VALUE_HIER3_ID_1, TEST_VALUE_HIER321_ID_2,
                TEST_VALUE_TAC, dateTime, 1); //2 impacted cells for this BSC
        insertData(TEST_VALUE_IMSI, TEST_VALUE_CATEGORY_ID, TEST_VALUE_HIER3_ID_2, TEST_VALUE_HIER321_ID_3,
                TEST_VALUE_TAC, dateTime, 1);
        insertData(TEST_VALUE_IMSI_1, TEST_VALUE_CATEGORY_ID, TEST_VALUE_HIER3_ID_2, TEST_VALUE_HIER321_ID_1,
                TEST_VALUE_TAC, dateTime, 1);//2 impacted subscribers for HIER3_ID_2 (TEST_VALUE_IMSI and TEST_VALUE_IMSI_1)
        insertData(TEST_VALUE_IMSI, TEST_VALUE_CATEGORY_ID, TEST_VALUE_HIER3_ID_3, TEST_VALUE_HIER321_ID_1,
                TEST_VALUE_TAC, dateTime, 1);//HIER3_ID_3 not in TEST_VALUE_BSC_GROUP so not included
        insertData(TEST_VALUE_IMSI_1, TEST_VALUE_CATEGORY_ID, TEST_VALUE_HIER3_ID_1, TEST_VALUE_HIER321_ID_1,
                SAMPLE_EXCLUSIVE_TAC, dateTime, 1);//exclusive tac not included
    }

    private void insertData(final String imsi, final String categoryID, final String controllerID, final String cellID,
            final int tac, final String time, final int instances) throws SQLException {
        for (int i = 0; i < instances; i++) {
            final Map<String, Object> dataForRawTable = new HashMap<String, Object>();
            dataForRawTable.put(HIER3_ID, controllerID);
            dataForRawTable.put(HIER321_ID, cellID);
            dataForRawTable.put(TAC, tac);
            dataForRawTable.put(DATETIME_ID, time);
            dataForRawTable.put(EVENT_TIME, time);
            dataForRawTable.put(TIMEZONE, "0");
            dataForRawTable.put(IMSI, imsi);
            dataForRawTable.put(CATEGORY_ID, categoryID);
            insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataForRawTable);
        }
    }

    private void insertAggregatedEventData() throws Exception {
        dateTime = DateTimeUtilities.getDateTimeMinusMinutes(40 + GSM_CFA_LATENCY_ON_ONE_DAY_QUERY);
        insertAggregatedRow(TEST_VALUE_HIER3_ID_1, TEST_VALUE_CATEGORY_ID, 15,30, dateTime);
        insertAggregatedRow(TEST_VALUE_HIER3_ID_2, TEST_VALUE_CATEGORY_ID, 7,14, dateTime);
        insertAggregatedRow(TEST_VALUE_HIER3_ID_3, TEST_VALUE_CATEGORY_ID, 2,4, dateTime);
    }

    private void insertAggregatedRow(final String controllerID, final String categoryID, final int numberOfErrors,final int numberOfSuccesses,
            final String time) throws SQLException {
        Map<String, Object> dataForAggTable = new HashMap<String, Object>();
        dataForAggTable.put(HIER3_ID, controllerID);
        dataForAggTable.put(CATEGORY_ID, categoryID);
        dataForAggTable.put(NO_OF_ERRORS, numberOfErrors);
        dataForAggTable.put(DATETIME_ID, time);
        insertRow(TEMP_EVENT_E_GSM_CFA_HIER3_EVENTID_ERR_15MIN, dataForAggTable);
        
        dataForAggTable = new HashMap<String, Object>();
        dataForAggTable.put(HIER3_ID, controllerID);
        dataForAggTable.put(CATEGORY_ID, categoryID);
        dataForAggTable.put(NO_OF_SUCCESSES, numberOfSuccesses);
        dataForAggTable.put(DATETIME_ID, time);
        
        insertRow(TEMP_EVENT_E_GSM_CFA_HIER3_EVENTID_SUC_15MIN, dataForAggTable);
    }

    private void createLookupTables() throws Exception {
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_EVENTTYPE, "DIM_E_GSM_CFA_EVENTTYPE", CATEGORY_ID,
                CATEGORY_ID_DESC);
        createAndReplaceLookupTable(TEMP_GROUP_TYPE_E_RAT_VEND_HIER3, "GROUP_TYPE_E_RAT_VEND_HIER3", GROUP_NAME_KEY,
                HIER3_ID);
        createAndReplaceLookupTable(TEMP_DIM_E_SGEH_HIER321, "DIM_E_SGEH_HIER321", HIER3_ID, HIERARCHY_3, VENDOR, RAT);
    }

    private void createAndReplaceLookupTable(final String tempTableName, final String tableNameToReplace,
            final String... columns) throws Exception {
        final Collection<String> columnsForTable = new ArrayList<String>();
        for (final String column : columns) {
            columnsForTable.add(column);
        }
        createTemporaryTable(tempTableName, columnsForTable);
        ReplaceTablesWithTempTablesTemplateUtils.addTableNameToReplace(tableNameToReplace, tempTableName);
    }

    private void insertLookupData() throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(CATEGORY_ID, TEST_VALUE_CATEGORY_ID);
        valuesForTable.put(CATEGORY_ID_DESC, TEST_VALUE_CATEGORY_ID_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_EVENTTYPE, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(GROUP_NAME_KEY, TEST_VALUE_BSC_GROUP);
        valuesForTable.put(HIER3_ID, TEST_VALUE_HIER3_ID_1);
        insertRow(TEMP_GROUP_TYPE_E_RAT_VEND_HIER3, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(GROUP_NAME_KEY, TEST_VALUE_BSC_GROUP);
        valuesForTable.put(HIER3_ID, TEST_VALUE_HIER3_ID_2);
        insertRow(TEMP_GROUP_TYPE_E_RAT_VEND_HIER3, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(GROUP_NAME_KEY, TEST_VALUE_BSC_CONTROL_GROUP);
        valuesForTable.put(HIER3_ID, TEST_VALUE_HIER3_ID_3);
        insertRow(TEMP_GROUP_TYPE_E_RAT_VEND_HIER3, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(HIER3_ID, TEST_VALUE_HIER3_ID_1);
        valuesForTable.put(HIERARCHY_3, SAMPLE_HIERARCHY_3);
        valuesForTable.put(RAT, RAT_INTEGER_VALUE_FOR_2G);
        valuesForTable.put(VENDOR, ERICSSON);
        insertRow(TEMP_DIM_E_SGEH_HIER321, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(HIER3_ID, TEST_VALUE_HIER3_ID_2);
        valuesForTable.put(HIERARCHY_3, "ONRM_ROOT_MO_R2:RNC01:RNC02");
        valuesForTable.put(RAT, RAT_INTEGER_VALUE_FOR_2G);
        valuesForTable.put(VENDOR, ERICSSON);
        insertRow(TEMP_DIM_E_SGEH_HIER321, valuesForTable);

    }

    private void insertDataIntoTacGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    private void createEventTable() throws Exception {
        Collection<String> columnsForEventTable = new ArrayList<String>();

        // Create aggregation table for error
        columnsForEventTable.add(HIER3_ID);
        columnsForEventTable.add(NO_OF_ERRORS);
        columnsForEventTable.add(CATEGORY_ID);
        columnsForEventTable.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_HIER3_EVENTID_ERR_15MIN, columnsForEventTable);
        
        // Create aggregation table for success
        columnsForEventTable = new ArrayList<String>();
        columnsForEventTable.add(HIER3_ID);
        columnsForEventTable.add(NO_OF_SUCCESSES);
        columnsForEventTable.add(CATEGORY_ID);
        columnsForEventTable.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_HIER3_EVENTID_SUC_15MIN, columnsForEventTable);

        // Create raw table for the impacted subscriber calculation
        columnsForEventTable.clear();
        columnsForEventTable.add(HIER3_ID);
        columnsForEventTable.add(HIER321_ID);
        columnsForEventTable.add(TAC);
        columnsForEventTable.add(DATETIME_ID);
        columnsForEventTable.add(EVENT_TIME);
        columnsForEventTable.add(TIMEZONE);
        columnsForEventTable.add(IMSI);
        columnsForEventTable.add(CATEGORY_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columnsForEventTable);
    }
}
