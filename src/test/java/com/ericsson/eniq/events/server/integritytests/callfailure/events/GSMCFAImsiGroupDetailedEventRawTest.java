/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2014
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.CallFailureSubscriberDetailedService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.GSMCallFailureSubscriberGroupDetailedEventAnalysisResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @since 2011
 *
 */
public class GSMCFAImsiGroupDetailedEventRawTest extends
        BaseDataIntegrityTest<GSMCallFailureSubscriberGroupDetailedEventAnalysisResult> {

    private static final String TEST_VALUE_CATEGORY_ID = GSM_CALL_DROP_CATEGORY_ID;

    private static final String TEST_VALUE_HIER321_ID = "4948639634796658772";

    private static final String TEST_VALUE_HIER3_ID = "5386564559998864911";

    private static final String TEST_VALUE_CAUSE_GROUP = "4";

    private static final String TEST_VALUE_CAUSE_GROUP_DESC = "LOW SS DOWNLINK";

    private static final String TEST_VALUE_EXTENDED_CAUSE = "61";

    private static final String TEST_VALUE_EXTENDED_DESC = "OTHER, PREEMPTION";

    private static final String TEST_VALUE_URGENCY_CONDITION = "1";

    private static final String TEST_VALUE_URGENCY_CONDITION_DESC = "Urgency1";

    private static final String TEST_VALUE_CATEGORY_ID_DESC = "Call Drops";

    private static final String TEST_VALUE_IMSIGROUP1 = "IMSIGroup1";

    private static final int TEST_VALUE_TAC = 100100;

    private static final String TEST_VALUE_MANUFACTURER = "Mitsubishi";

    private static final String TEST_VALUE_MARKETING_NAME = "G410";

    private static final int TEST_VALUE_RELEASE_TYPE = 0;

    private static final String TEST_VALUE_RELEASE_TYPE_DESC = "(MSC) NORMAL RELEASE";

    private static final String TEST_IMSI_1 = "11111119";

    private static final String TEST_IMSI_2 = "22222229";

    private static final String TEST_IMSI_3 = "33333339";

    private static final String TEST_VAMOS_NEIGHBOR_INDICATOR = "1";

    private static final String TEST_VAMOS_PAIR_ALLOCATION_BY_MS = "Neighbor1";

    private static final String TEST_RSAI = "1";

    private static final String TEST_RSAI_DESC = "RSAI1";

    private static final String TEST_CHANNEL_TYPE = "1";

    private static final String TEST_CHANNEL_TYPE_DESC = "Channel1";

    private static final String TEST_MSISDN = "123456";

    private static final String TEST_VALUE_VENDOR = "Ericsson";

    private CallFailureSubscriberDetailedService service;

    @Before
    public void setup() throws Exception {
        service = new CallFailureSubscriberDetailedService();
        attachDependencies(service);
        createEventTable();
        createLookupTables();
        insertLookupData();
        insertDataIntoTacGroupTable();
    }

    @Test
    public void testFiveMinuteQuery() throws URISyntaxException, Exception {
        insertEventData(DateTimeUtilities.getDateTimeMinus5Minutes());
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TIME_QUERY_PARAM, FIVE_MINUTES);
        requestParameters.add(GROUP_NAME_PARAM, TEST_VALUE_IMSIGROUP1);
        requestParameters.add(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID);
        requestParameters.add(TYPE_PARAM, TYPE_IMSI);
        requestParameters.add(IMSI, TEST_IMSI_1);
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
        requestParameters.add(TYPE_PARAM, TYPE_IMSI);
        requestParameters.add(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID);
        requestParameters.add(IMSI, TEST_IMSI_1);
        final String result = runQuery(service, requestParameters);
        verifyResult(result);
    }

    private void verifyResult(final String json) throws Exception {
        assertJSONSucceeds(json);
        validateAgainstGridDefinition(json, "GSM_SUBSCRIBER_DETAILED_EVENT_ANALYSIS_BY_GROUP");
        final List<GSMCallFailureSubscriberGroupDetailedEventAnalysisResult> results = getTranslator().translateResult(
                json, GSMCallFailureSubscriberGroupDetailedEventAnalysisResult.class);

        assertThat(results.size(), is(3));
        for (final GSMCallFailureSubscriberGroupDetailedEventAnalysisResult result : results) {
            assertThat(result.getEventType(), is(TEST_VALUE_CATEGORY_ID_DESC));
            assertThat(result.getController(), is(TEST_VALUE_GSM_CONTROLLER1_NAME));
            assertThat(result.getVendor(), is(TEST_VALUE_VENDOR));
            assertThat(result.getEventType(), is(TEST_VALUE_CATEGORY_ID_DESC));
            assertThat(result.getReleaseType(), is(TEST_VALUE_RELEASE_TYPE_DESC));
            assertThat(result.getTac(), is(TEST_VALUE_TAC));
            assertThat(result.getTerminalMake(), is(TEST_VALUE_MANUFACTURER));
            assertThat(result.getTerminalModel(), is(TEST_VALUE_MARKETING_NAME));
            assertThat(result.getRsai(), is(TEST_RSAI_DESC));
            assertThat(result.getChannel(), is(TEST_CHANNEL_TYPE_DESC));
            assertThat(result.getVamos(), is(TEST_VAMOS_PAIR_ALLOCATION_BY_MS));
            assertThat(result.getUrgency(), is(TEST_VALUE_URGENCY_CONDITION_DESC));
            assertThat(result.getCause(), is(TEST_VALUE_CAUSE_GROUP_DESC));
            assertThat(result.getExCause(), is(TEST_VALUE_EXTENDED_DESC));
            assertThat(result.getImsi(), is(TEST_IMSI_1));
        }
    }

    private void insertEventData(final String dateTime) throws Exception {
        insertData(TEST_IMSI_1, TEST_VALUE_CATEGORY_ID, TEST_VALUE_TAC, dateTime, 3);
        insertData(TEST_IMSI_2, TEST_VALUE_CATEGORY_ID, TEST_VALUE_TAC, dateTime, 4);
        insertData(TEST_IMSI_3, TEST_VALUE_CATEGORY_ID, SAMPLE_EXCLUSIVE_TAC, dateTime, 1);
    }

    private void insertData(final String imsi, final String categoryId, final int tac, final String time,
            final int instances) throws SQLException {
        for (int i = 0; i < instances; i++) {
            final Map<String, Object> dataForEventTable = new HashMap<String, Object>();
            dataForEventTable.put(HIER3_ID, TEST_VALUE_HIER3_ID);
            dataForEventTable.put(HIER321_ID, TEST_VALUE_HIER321_ID);
            dataForEventTable.put(TAC, tac);
            dataForEventTable.put(CAUSE_GROUP, TEST_VALUE_CAUSE_GROUP);
            dataForEventTable.put(URGENCY_CONDITION, TEST_VALUE_URGENCY_CONDITION);
            dataForEventTable.put(EXTENDED_CAUSE, TEST_VALUE_EXTENDED_CAUSE);
            dataForEventTable.put(RELEASE_TYPE, TEST_VALUE_RELEASE_TYPE);
            dataForEventTable.put(EVENT_TIME, time);
            dataForEventTable.put(TIMEZONE, "0");
            dataForEventTable.put(IMSI, imsi);
            dataForEventTable.put(CATEGORY_ID, categoryId);
            dataForEventTable.put(DATETIME_ID, time);
            dataForEventTable.put(RSAI, TEST_RSAI);
            dataForEventTable.put(VAMOS_NEIGHBOR_INDICATOR, TEST_VAMOS_NEIGHBOR_INDICATOR);
            dataForEventTable.put(CHANNEL_TYPE, TEST_CHANNEL_TYPE);
            dataForEventTable.put(MSISDN, TEST_MSISDN);
            insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataForEventTable);
        }
    }

    private void createLookupTables() throws Exception {
        createAndReplaceLookupTable(TEMP_DIM_E_SGEH_TAC, DIM_E_SGEH_TAC, MANUFACTURER, MARKETING_NAME, TAC);
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_EVENTTYPE, "DIM_E_GSM_CFA_EVENTTYPE", CATEGORY_ID,
                CATEGORY_ID_DESC);
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_URGENCY_CONDITION, "DIM_E_GSM_CFA_URGENCY_CONDITION",
                URGENCY_CONDITION, URGENCY_CONDITION_DESC);
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_RELEASE_TYPE, "DIM_E_GSM_CFA_RELEASE_TYPE", RELEASE_TYPE,
                RELEASE_TYPE_DESC);
        createAndReplaceLookupTable(TEMP_DIM_E_SGEH_HIER321, "DIM_E_SGEH_HIER321", HIERARCHY_1, HIERARCHY_3, VENDOR,
                HIER321_ID, HIER3_ID, RAT);
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_VAMOS_NEIGHBOR_INDICATOR,
                "DIM_E_GSM_CFA_VAMOS_NEIGHBOR_INDICATOR", VAMOS_NEIGHBOR_INDICATOR, VAMOS_PAIR_ALLOCATION_BY_MS);
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_RSAI, "DIM_E_GSM_CFA_RSAI", RSAI, RSAI_DESC);
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_CHANNEL_TYPE, "DIM_E_GSM_CFA_CHANNEL_TYPE", CHANNEL_TYPE,
                CHANNEL_TYPE_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_EXTENDED_CAUSE, "DIM_E_GSM_CFA_EXTENDED_CAUSE", EXTENDED_CAUSE,
                EXTENDED_CAUSE_DESC);
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_CAUSE_GROUP, "DIM_E_GSM_CFA_CAUSE_GROUP", CAUSE_GROUP,
                CAUSE_GROUP_DESC);
        createAndReplaceLookupTable(TEMP_GROUP_TYPE_E_IMSI, "GROUP_TYPE_E_IMSI", GROUP_NAME, IMSI);
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
        valuesForTable.put(MANUFACTURER, TEST_VALUE_MANUFACTURER);
        valuesForTable.put(MARKETING_NAME, TEST_VALUE_MARKETING_NAME);
        valuesForTable.put(TAC, TEST_VALUE_TAC);
        insertRow(TEMP_DIM_E_SGEH_TAC, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID);
        valuesForTable.put(CATEGORY_ID_DESC, TEST_VALUE_CATEGORY_ID_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_EVENTTYPE, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(URGENCY_CONDITION, TEST_VALUE_CAUSE_GROUP);
        valuesForTable.put(URGENCY_CONDITION_DESC, TEST_VALUE_CAUSE_GROUP_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_URGENCY_CONDITION, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(EXTENDED_CAUSE, TEST_VALUE_EXTENDED_CAUSE);
        valuesForTable.put(EXTENDED_CAUSE_DESC, TEST_VALUE_EXTENDED_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_EXTENDED_CAUSE, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(RELEASE_TYPE, TEST_VALUE_RELEASE_TYPE);
        valuesForTable.put(RELEASE_TYPE_DESC, TEST_VALUE_RELEASE_TYPE_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_RELEASE_TYPE, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(HIERARCHY_3, TEST_VALUE_GSM_CONTROLLER1_NAME);
        valuesForTable.put(HIER321_ID, TEST_VALUE_HIER321_ID);
        valuesForTable.put(HIERARCHY_1, TEST_VALUE_GSM_CELL1_NAME);
        valuesForTable.put(HIER3_ID, TEST_VALUE_HIER3_ID);
        valuesForTable.put(VENDOR, TEST_VALUE_VENDOR);
        valuesForTable.put(RAT, "0");
        insertRow(TEMP_DIM_E_SGEH_HIER321, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(VAMOS_NEIGHBOR_INDICATOR, TEST_VAMOS_NEIGHBOR_INDICATOR);
        valuesForTable.put(VAMOS_PAIR_ALLOCATION_BY_MS, TEST_VAMOS_PAIR_ALLOCATION_BY_MS);
        insertRow(TEMP_DIM_E_GSM_CFA_VAMOS_NEIGHBOR_INDICATOR, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(RSAI, TEST_RSAI);
        valuesForTable.put(RSAI_DESC, TEST_RSAI_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_RSAI, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(CHANNEL_TYPE, TEST_CHANNEL_TYPE);
        valuesForTable.put(CHANNEL_TYPE_DESC, TEST_CHANNEL_TYPE_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_CHANNEL_TYPE, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(URGENCY_CONDITION, TEST_VALUE_URGENCY_CONDITION);
        valuesForTable.put(URGENCY_CONDITION_DESC, TEST_VALUE_URGENCY_CONDITION_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_URGENCY_CONDITION, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(CAUSE_GROUP, TEST_VALUE_CAUSE_GROUP);
        valuesForTable.put(CAUSE_GROUP_DESC, TEST_VALUE_CAUSE_GROUP_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_CAUSE_GROUP, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(IMSI, TEST_IMSI_1);
        valuesForTable.put(GROUP_NAME, TEST_VALUE_IMSIGROUP1);
        insertRow(TEMP_GROUP_TYPE_E_IMSI, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(IMSI, TEST_IMSI_2);
        valuesForTable.put(GROUP_NAME, TEST_VALUE_IMSIGROUP1);
        insertRow(TEMP_GROUP_TYPE_E_IMSI, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(IMSI, TEST_IMSI_3);
        valuesForTable.put(GROUP_NAME, TEST_VALUE_IMSIGROUP1);
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
        columnsForEventTable.add(HIER3_ID);
        columnsForEventTable.add(HIER321_ID);
        columnsForEventTable.add(TAC);
        columnsForEventTable.add(URGENCY_CONDITION);
        columnsForEventTable.add(EXTENDED_CAUSE);
        columnsForEventTable.add(RELEASE_TYPE);
        columnsForEventTable.add(EVENT_TIME);
        columnsForEventTable.add(TIMEZONE);
        columnsForEventTable.add(IMSI);
        columnsForEventTable.add(CATEGORY_ID);
        columnsForEventTable.add(DATETIME_ID);
        columnsForEventTable.add(RSAI);
        columnsForEventTable.add(VAMOS_NEIGHBOR_INDICATOR);
        columnsForEventTable.add(CHANNEL_TYPE);
        columnsForEventTable.add(MSISDN);
        columnsForEventTable.add(CAUSE_GROUP);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columnsForEventTable);
    }

}
