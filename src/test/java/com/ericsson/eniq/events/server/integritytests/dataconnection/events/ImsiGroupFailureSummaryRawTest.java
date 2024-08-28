/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.dataconnection.events;

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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.dataconnection.events.SubscriberGroupSummaryService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.dataconnection.ImsiGroupFailureSummaryResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author eatiaro
 * @since 2011
 *
 */
public class ImsiGroupFailureSummaryRawTest extends BaseDataIntegrityTest<ImsiGroupFailureSummaryResult> {

    private SubscriberGroupSummaryService service;

    @Before
    public void setup() throws Exception {
        service = new SubscriberGroupSummaryService();
        attachDependencies(service);
        createEventTable();
        createLookupTables();
        insertLookupData();
        insertGroupData();
        insertDataIntoTacGroupTable();
    }

    @Test
    public void testFiveMinuteQuery() throws URISyntaxException, Exception {
        insertEventData(DateTimeUtilities.getDateTimeMinusMinutes(2));
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TIME_QUERY_PARAM, FIVE_MINUTES);
        requestParameters.add(SEARCH_PARAM, TEST_VALUE_IMSIGROUP1);
        final String result = runQuery(service, requestParameters);
        verifyResult(result);
    }

    @Test
    public void testTwoHourQuery() throws URISyntaxException, Exception {
        insertEventData(DateTimeUtilities.getDateTimeMinusMinutes(10));
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TIME_QUERY_PARAM, "120");
        requestParameters.add(SEARCH_PARAM, TEST_VALUE_IMSIGROUP1);
        final String result = runQuery(service, requestParameters);
        verifyResult(result);
    }

    private void verifyResult(final String json) throws Exception {
        assertJSONSucceeds(json);
        validateAgainstGridDefinition(json, "GSM_PS_SUBSCRIBER_SUMMARY_EVENT_ANALYSIS_BY_IMSI_GROUP");
        final List<ImsiGroupFailureSummaryResult> results = getTranslator().translateResult(json,
                ImsiGroupFailureSummaryResult.class);

        assertThat(results.size(), is(1));
        final ImsiGroupFailureSummaryResult result = results.get(0);
        assertThat(result.getEventType(), is(TEST_VALUE_CATEGORY_ID_DESC));
        //Three failures, all for IMSI1
        assertThat(result.getFailures(), is(3));
        //One impacted subscriber IMSI1
        assertThat(result.getImpactedSubscribers(), is(1));
    }

    private void insertEventData(final String dateTime) throws Exception {
        insertData(TEST_VALUE_IMSI1, TEST_VALUE_TAC, dateTime, 3);
        insertData(TEST_VALUE_IMSI3, TEST_VALUE_TAC, dateTime, 5);
        insertData(TEST_VALUE_IMSI1, SAMPLE_EXCLUSIVE_TAC, dateTime, 1);
    }

    private void insertData(final String imsi, final int tac, final String time, final int instances)
            throws SQLException {
        final Map<String, Object> dataForEventTable = new HashMap<String, Object>();
        for (int i = 0; i < instances; i++) {
            dataForEventTable.clear();
            dataForEventTable.put(TAC, tac);
            dataForEventTable.put(EVENT_TIME, time);
            dataForEventTable.put(TIMEZONE, "0");
            dataForEventTable.put(IMSI, imsi);
            dataForEventTable.put(CATEGORY_ID, TEST_VALUE_CATEGORY_ID);
            dataForEventTable.put(DATETIME_ID, time);
            insertRow(TEMP_EVENT_E_GSM_PS_ERR_RAW, dataForEventTable);
        }
    }

    private void createLookupTables() throws Exception {
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_EVENTTYPE, DIM_E_GSM_PS_EVENTTYPE, CATEGORY_ID, CATEGORY_ID_DESC);

        createAndReplaceLookupTable(TEMP_GROUP_TYPE_E_IMSI, GROUP_TYPE_E_IMSI, GROUP_NAME, IMSI);
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

    private void insertGroupData() throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(GROUP_NAME, TEST_VALUE_IMSIGROUP1);
        valuesForTable.put(IMSI, TEST_VALUE_IMSI1);
        insertRow(TEMP_GROUP_TYPE_E_IMSI, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(GROUP_NAME, TEST_VALUE_IMSIGROUP1);
        valuesForTable.put(IMSI, TEST_VALUE_IMSI2);
        insertRow(TEMP_GROUP_TYPE_E_IMSI, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(GROUP_NAME, TEST_VALUE_IMSIGROUP2);
        valuesForTable.put(IMSI, TEST_VALUE_IMSI3);
        insertRow(TEMP_GROUP_TYPE_E_IMSI, valuesForTable);
    }

    private void insertLookupData() throws SQLException {
        final Map<String, Object> lookuptableValues = new HashMap<String, Object>();
        lookuptableValues.put(CATEGORY_ID, TEST_VALUE_CATEGORY_ID);
        lookuptableValues.put(CATEGORY_ID_DESC, TEST_VALUE_CATEGORY_ID_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_EVENTTYPE, lookuptableValues);
        lookuptableValues.clear();
        lookuptableValues.put(CATEGORY_ID, TEST_VALUE_CATEGORY_ID_CONTROL);
        lookuptableValues.put(CATEGORY_ID_DESC, TEST_VALUE_CATEGORY_ID_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_EVENTTYPE, lookuptableValues);
    }

    private void insertDataIntoTacGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    private void createEventTable() throws Exception {
        final Collection<String> columnsForEventTable = new ArrayList<String>();
        columnsForEventTable.add(TAC);
        columnsForEventTable.add(EVENT_TIME);
        columnsForEventTable.add(TIMEZONE);
        columnsForEventTable.add(IMSI);
        columnsForEventTable.add(CATEGORY_ID);
        columnsForEventTable.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_PS_ERR_RAW, columnsForEventTable);
    }

    private static String TEST_VALUE_CATEGORY_ID = "2";

    private static final String TEST_VALUE_CATEGORY_ID_DESC = "Connection Failures";

    private static final String TEST_VALUE_IMSI1 = "46000608201336";

    private static final String TEST_VALUE_IMSI2 = "53000608201337";

    private static final String TEST_VALUE_IMSI3 = "1298608201337";

    private static final int TEST_VALUE_TAC = 100100;

    private static final String TEST_VALUE_CATEGORY_ID_CONTROL = "0";

    private static final String TEST_VALUE_CATEGORY_ID_DESC_CONTROL = "Dummy value";

    private static final String TEST_VALUE_IMSIGROUP1 = "IMSIGroup1";

    private static final String TEST_VALUE_IMSIGROUP2 = "IMSIGroup2";

}
