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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.SubscriberGroupBreakdownService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.SubscriberGroupBreakdownResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author eatiaro
 * @since 2012
 *
 */
public class SubscriberGroupSummaryBreakdownRawTest extends BaseDataIntegrityTest<SubscriberGroupBreakdownResult> {

    private SubscriberGroupBreakdownService service;

    @Before
    public void setup() throws Exception {
        service = new SubscriberGroupBreakdownService();
        attachDependencies(service);
        createEventTable();
        createLookupTables();
        insertGroupData();
        insertDataIntoTacGroupTable();
    }

    @Test
    public void testFiveMinuteQuery() throws URISyntaxException, Exception {
        insertEventData(DateTimeUtilities.getDateTimeMinusMinutes(2));
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TIME_QUERY_PARAM, FIVE_MINUTES);
        requestParameters.add(GROUP_NAME_PARAM, TEST_VALUE_IMSIGROUP1);
        requestParameters.add(CATEGORY_ID_DESC_PARAM, TEST_VALUE_CATEGORY_ID_DESC);
        requestParameters.add(CATEGORY_ID, TEST_VALUE_CATEGORY_ID + "");
        requestParameters.add(TYPE_PARAM, TYPE_IMSI);
        final String result = runQuery(service, requestParameters);
        verifyResult(result);
    }

    @Test
    public void testTwoHourQuery() throws URISyntaxException, Exception {
        insertEventData(DateTimeUtilities.getDateTimeMinusMinutes(10));
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TIME_QUERY_PARAM, "120");
        requestParameters.add(GROUP_NAME_PARAM, TEST_VALUE_IMSIGROUP1);
        requestParameters.add(CATEGORY_ID, TEST_VALUE_CATEGORY_ID + "");
        requestParameters.add(CATEGORY_ID_DESC_PARAM, TEST_VALUE_CATEGORY_ID_DESC);
        requestParameters.add(TYPE_PARAM, TYPE_IMSI);
        final String result = runQuery(service, requestParameters);
        verifyResult(result);
    }

    private void verifyResult(final String json) throws Exception {
        assertJSONSucceeds(json);
        validateAgainstGridDefinition(json, "GSM_SUBSCRIBER_GROUP_BREAKDOWN");
        final List<SubscriberGroupBreakdownResult> results = getTranslator().translateResult(json,
                SubscriberGroupBreakdownResult.class);

        assertThat(results.size(), is(2));//for IMSIs 1 and 5

        final SubscriberGroupBreakdownResult result1 = results.get(0);
        final SubscriberGroupBreakdownResult result2 = results.get(1);

        assertThat(result1.getCategoryDescription(), is(TEST_VALUE_CATEGORY_ID_DESC));
        assertThat(result2.getCategoryDescription(), is(TEST_VALUE_CATEGORY_ID_DESC));

        final Map<String, Integer> imsiToFailure = new HashMap<String, Integer>();
        imsiToFailure.put(result1.getImsi(), result1.getNumberOfFailures());
        imsiToFailure.put(result2.getImsi(), result2.getNumberOfFailures());

        assertThat(imsiToFailure.get(TEST_VALUE_IMSI1), is(3));//we inserted 3 failures for imsi1
        assertThat(imsiToFailure.get(TEST_VALUE_IMSI5), is(4));//we inserted 4 failures for imsi5

        assertThat(result1.getRatio(), is(50.00));
        assertThat(result2.getRatio(), is(50.00));
    }

    private void insertEventData(final String dateTime) throws Exception {
        insertData(TEST_VALUE_IMSI1, TEST_VALUE_TAC, TEST_VALUE_CATEGORY_ID, dateTime, 3);
        insertData(TEST_VALUE_IMSI3, TEST_VALUE_TAC, TEST_VALUE_CATEGORY_ID, dateTime, 5);
        insertData(TEST_VALUE_IMSI4, TEST_VALUE_TAC, TEST_VALUE_CATEGORY_ID_CONTROL, dateTime, 3);
        insertData(TEST_VALUE_IMSI5, TEST_VALUE_TAC, TEST_VALUE_CATEGORY_ID, dateTime, 4);
        insertData(TEST_VALUE_IMSI1, SAMPLE_EXCLUSIVE_TAC, TEST_VALUE_CATEGORY_ID, dateTime, 1);
    }

    private void insertData(final String imsi, final int tac, final int categoryID, final String time,
            final int instances) throws SQLException {
        final Map<String, Object> dataForEventTable = new HashMap<String, Object>();
        for (int i = 0; i < instances; i++) {
            dataForEventTable.clear();
            dataForEventTable.put(TAC, tac);
            dataForEventTable.put(EVENT_TIME, time);
            dataForEventTable.put(TIMEZONE, "0");
            dataForEventTable.put(IMSI, imsi);
            dataForEventTable.put(CATEGORY_ID, categoryID);
            dataForEventTable.put(DATETIME_ID, time);
            insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataForEventTable);
            dataForEventTable.remove(CATEGORY_ID);
            insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataForEventTable);
        }
    }

    private void createLookupTables() throws Exception {
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
        valuesForTable.put(GROUP_NAME, TEST_VALUE_IMSIGROUP1);
        valuesForTable.put(IMSI, TEST_VALUE_IMSI4);
        insertRow(TEMP_GROUP_TYPE_E_IMSI, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(GROUP_NAME, TEST_VALUE_IMSIGROUP1);
        valuesForTable.put(IMSI, TEST_VALUE_IMSI5);
        insertRow(TEMP_GROUP_TYPE_E_IMSI, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(GROUP_NAME, TEST_VALUE_IMSIGROUP2);
        valuesForTable.put(IMSI, TEST_VALUE_IMSI3);
        insertRow(TEMP_GROUP_TYPE_E_IMSI, valuesForTable);
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
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columnsForEventTable);
        columnsForEventTable.remove(CATEGORY_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_SUC_RAW, columnsForEventTable);
    }

    private static int TEST_VALUE_CATEGORY_ID = 0;

    private static final int TEST_VALUE_CATEGORY_ID_CONTROL = 7;

    private static final String TEST_VALUE_CATEGORY_ID_DESC = "Call Drops";

    private static final String TEST_VALUE_IMSI1 = "46000608201336";

    private static final String TEST_VALUE_IMSI2 = "53000608201337";

    private static final String TEST_VALUE_IMSI3 = "1298608201337";

    private static final String TEST_VALUE_IMSI4 = "9867608200000";

    private static final String TEST_VALUE_IMSI5 = "9867608201234";

    private static final int TEST_VALUE_TAC = 100100;

    private static final String TEST_VALUE_IMSIGROUP1 = "IMSIGroup1";

    private static final String TEST_VALUE_IMSIGROUP2 = "IMSIGroup2";

}
