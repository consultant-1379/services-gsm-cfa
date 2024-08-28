/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.dataconnection.cc;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.integritytests.stubs.ReplaceTablesWithTempTablesTemplateUtils;
import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.dataconnection.cc.SubscriberSuccessCCService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.dataconnection.SuccessCauseCodePieChartResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ejoegaf
 * @since 2012
 *
 */
public class IMSISuccessCauseCodeRawTest extends BaseDataIntegrityTest<SuccessCauseCodePieChartResult> {

    private static final int TEST_VALUE_TBF_RELEASE_CAUSE_0 = 0;

    private static final String TEST_VALUE_TBF_RELEASE_CAUSE_0_DESC = "Normal release";

    private static final int TEST_VALUE_TBF_RELEASE_CAUSE_2 = 2;

    private static final String TEST_VALUE_TBF_RELEASE_CAUSE_2_DESC = "TBF released due to missing radio contact or removal of resources (no response from BTS after Immediate Assignment have been sent, timing advance channel faulty, no answer from the MS, essential channel removed)";

    private static final int TEST_VALUE_TBF_RELEASE_CAUSE_3 = 3;

    private static final String TEST_VALUE_TBF_RELEASE_CAUSE_3_DESC = "TBF released due to N3105 DL (too many consecutive unanswered polling requests), and TBF released due to N3103 UL (no reply from the MS after sending the final PACKET UPLINK ACK/NACK message)";

    private static final int TEST_VALUE_TBF_RELEASE_CAUSE_4 = 4;

    private static final String TEST_VALUE_TBF_RELEASE_CAUSE_4_DESC = "TBF released due to Flush";

    private static final String TEST_VALUE_IMSI = "460006082013326";

    private static final String TEST_VALUE_IMSI_2 = "460006082013327";

    private static final int TEST_VALUE_TAC = 100100;

    private SubscriberSuccessCCService imsiService;

    @Before
    public void setup() throws Exception {
        createEventTable();
        createLookupTables();
        insertDataIntoTacGroupTable();
        insertAllLookupData();
    }

    @Test
    public void testIMSIFiveMinuteQuery() throws URISyntaxException, Exception {
        insertEventData(GSM_CFA_LATENCY_ON_FIVE_MIN_QUERY);
        imsiService = new SubscriberSuccessCCService();
        attachDependencies(imsiService);
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TIME_QUERY_PARAM, FIVE_MINUTES);
        requestParameters.add(IMSI_PARAM, TEST_VALUE_IMSI);
        requestParameters.add(TYPE_PARAM, TYPE_IMSI);
        final String result = runQuery(imsiService, requestParameters);
        verifyResult(result);
    }

    @Test
    public void testIMSIOneDayQuery() throws URISyntaxException, Exception {
        //same as 5 min test - just to verify it still goes to raw table
        insertEventData(GSM_CFA_LATENCY_ON_ONE_DAY_QUERY);
        imsiService = new SubscriberSuccessCCService();
        attachDependencies(imsiService);
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TIME_QUERY_PARAM, ONE_DAY);
        requestParameters.add(IMSI_PARAM, TEST_VALUE_IMSI);
        requestParameters.add(TYPE_PARAM, TYPE_IMSI);
        final String result = runQuery(imsiService, requestParameters);
        verifyResult(result);
    }

    private void verifyResult(final String json) throws Exception {
        assertJSONSucceeds(json);
        final List<SuccessCauseCodePieChartResult> results = getTranslator().translateResult(json,
                SuccessCauseCodePieChartResult.class);

        Collections.sort(results);

        assertThat(results.size(), is(3));//success included, and 2 other cause codes

        assertThat(results.get(0).getCauseCodeId(), is(TEST_VALUE_TBF_RELEASE_CAUSE_0));
        assertThat(results.get(0).getCauseCodeDesc(), is(TEST_VALUE_TBF_RELEASE_CAUSE_0_DESC));
        assertThat(results.get(0).getNoOccurrences(), is(8));

        assertThat(results.get(1).getCauseCodeId(), is(TEST_VALUE_TBF_RELEASE_CAUSE_2));
        assertThat(results.get(1).getCauseCodeDesc(), is(TEST_VALUE_TBF_RELEASE_CAUSE_2_DESC));
        assertThat(results.get(1).getNoOccurrences(), is(2));

        assertThat(results.get(2).getCauseCodeId(), is(TEST_VALUE_TBF_RELEASE_CAUSE_4));
        assertThat(results.get(2).getCauseCodeDesc(), is(TEST_VALUE_TBF_RELEASE_CAUSE_4_DESC));
        assertThat(results.get(2).getNoOccurrences(), is(5));
    }

    private void insertDataIntoTacGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    private void insertEventData(final int latency) throws Exception {
        final String dateTime2mins = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.MINUTE, -2 - latency);
        final String dateTime3mins = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.MINUTE, -3 - latency);
        final String dateTime48Hours = DateTimeUtilities.getDateTimeMinus48Hours();

        insertData(TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_0, dateTime2mins, 1, TEST_VALUE_TAC);
        insertData(TEST_VALUE_IMSI_2, TEST_VALUE_TBF_RELEASE_CAUSE_0, dateTime2mins, 2, TEST_VALUE_TAC);//exclude - wrong imsi
        insertData(TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_0, dateTime3mins, 4, TEST_VALUE_TAC);
        insertData(TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_0, dateTime48Hours, 9, TEST_VALUE_TAC);//exclude - time
        insertData(TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_0, dateTime3mins, 3, SAMPLE_EXCLUSIVE_TAC);//do not exclude exclusive tac in IMSI query
        insertData(TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_2, dateTime2mins, 2, TEST_VALUE_TAC);
        insertData(TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_3, dateTime2mins, 0, TEST_VALUE_TAC);//exclude - 0 successes
        insertData(TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_4, dateTime2mins, 5, TEST_VALUE_TAC);
    }

    private void insertData(final String imsi, final int causeCode, final String time, final int instances,
            final int tac) throws SQLException {
        final Map<String, Object> dataForEventTable = new HashMap<String, Object>();
        dataForEventTable.put(NO_OF_SUCCESSES, instances);
        dataForEventTable.put(TAC, tac);
        dataForEventTable.put(TBF_RELEASE_CAUSE, causeCode);
        dataForEventTable.put(DATETIME_ID, time);
        dataForEventTable.put(IMSI, imsi);
        insertRow(TEMP_EVENT_E_GSM_PS_ALL_RAW, dataForEventTable);
    }

    private void createLookupTables() throws Exception {
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_TBF_RELEASE_CAUSE, "DIM_E_GSM_PS_TBF_RELEASE_CAUSE",
                TBF_RELEASE_CAUSE, TBF_RELEASE_CAUSE_DESC);
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

    private void insertAllLookupData() throws SQLException {
        insertUrgencyConditionLookupData();
    }

    private void insertRowToUrgencyConditionTable(final int urgencyCondition, final String urgencyConditionDescription)
            throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(TBF_RELEASE_CAUSE, urgencyCondition);
        valuesForTable.put(TBF_RELEASE_CAUSE_DESC, urgencyConditionDescription);
        insertRow(TEMP_DIM_E_GSM_PS_TBF_RELEASE_CAUSE, valuesForTable);
    }

    private void insertUrgencyConditionLookupData() throws SQLException {
        insertRowToUrgencyConditionTable(TEST_VALUE_TBF_RELEASE_CAUSE_0, TEST_VALUE_TBF_RELEASE_CAUSE_0_DESC);
        insertRowToUrgencyConditionTable(TEST_VALUE_TBF_RELEASE_CAUSE_2, TEST_VALUE_TBF_RELEASE_CAUSE_2_DESC);
        insertRowToUrgencyConditionTable(TEST_VALUE_TBF_RELEASE_CAUSE_3, TEST_VALUE_TBF_RELEASE_CAUSE_3_DESC);
        insertRowToUrgencyConditionTable(TEST_VALUE_TBF_RELEASE_CAUSE_4, TEST_VALUE_TBF_RELEASE_CAUSE_4_DESC);
    }

    private void createEventTable() throws Exception {
        final Collection<String> columnsForEventTable = new ArrayList<String>();
        columnsForEventTable.add(NO_OF_SUCCESSES);
        columnsForEventTable.add(TBF_RELEASE_CAUSE);
        columnsForEventTable.add(TAC);
        columnsForEventTable.add(IMSI);
        columnsForEventTable.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_PS_ALL_RAW, columnsForEventTable);
    }

}
