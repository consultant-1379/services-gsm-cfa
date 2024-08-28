/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.integritytests.stubs.ReplaceTablesWithTempTablesTemplateUtils;
import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.dataconnection.cc.SubscriberSCCService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.dataconnection.SubCauseCodePieChartResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ejoegaf
 * @since 2012
 *
 */
public class IMSISubCauseCodeRawTest extends BaseDataIntegrityTest<SubCauseCodePieChartResult> {

    private static final String TEST_VALUE_TBF_RELEASE_CAUSE_0 = "0";

    private static final String TEST_VALUE_TBF_RELEASE_CAUSE_2 = "2";

    private static final String TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_0 = "0";

    private static final String TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_0_DESC = "Sub Cause 0";

    private static final String TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_2 = "2";

    private static final String TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_2_DESC = "Sub Cause 1";

    private static final String TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_3 = "3";

    private static final String TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_3_DESC = "Sub Cause 2";

    private static final String TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_4 = "4";

    private static final String TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_4_DESC = "Sub Cause 3";

    private static final String TEST_VALUE_IMSI = "460006082013326";

    private static final String TEST_VALUE_IMSI_2 = "460006082013327";

    private static final int TEST_VALUE_TAC = 100100;

    private SubscriberSCCService imsiSubCauseCodeService;

    @Before
    public void setup() throws Exception {
        createEventTable();
        createLookupTables();
        insertDataIntoTacGroupTable();
        insertAllLookupData();
        insertEventData();
    }

    @Test
    public void testIMSIFiveMinuteQuery() throws URISyntaxException, Exception {
        final String result = runFiveMinSubCauseCodeQuery();
        verifyResult(result);
    }

    private String runFiveMinSubCauseCodeQuery() throws URISyntaxException, Exception {
        imsiSubCauseCodeService = new SubscriberSCCService();
        attachDependencies(imsiSubCauseCodeService);
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TIME_QUERY_PARAM, FIVE_MINUTES);
        requestParameters.add(IMSI_PARAM, TEST_VALUE_IMSI);
        requestParameters.add(TYPE_PARAM, TYPE_IMSI);
        return runQuery(imsiSubCauseCodeService, requestParameters);
    }

    private void verifyResult(final String json) throws Exception {
        assertJSONSucceeds(json);
        final List<SubCauseCodePieChartResult> results = getTranslator().translateResult(json,
                SubCauseCodePieChartResult.class);

        assertThat(results.size(), is(2));

        assertThat(results.get(0).getSubCauseCodeId(), is(TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_0));
        assertThat(results.get(0).getSubCauseCodeDesc(), is(TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_0_DESC));
        assertThat(results.get(0).getNoOccurrences(), is(4));
        assertThat(results.get(0).getNoImpactedSubscribers(), is(1));

        assertThat(results.get(1).getSubCauseCodeId(), is(TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_4));
        assertThat(results.get(1).getSubCauseCodeDesc(), is(TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_4_DESC));
        assertThat(results.get(1).getNoOccurrences(), is(3));
        assertThat(results.get(1).getNoImpactedSubscribers(), is(1));
    }

    private void insertDataIntoTacGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    private void insertEventData() throws Exception {
        final String dateTime2mins = DateTimeUtilities.getDateTimeMinus2Minutes();
        final String dateTime3mins = DateTimeUtilities.getDateTimeMinus3Minutes();
        final String dateTime48Hours = DateTimeUtilities.getDateTimeMinus48Hours();
        //success events - test they are not counted as errors
        insertData(true, TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_2,
                TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_0, dateTime2mins, 1, TEST_VALUE_TAC);
        insertData(true, TEST_VALUE_IMSI_2, TEST_VALUE_TBF_RELEASE_CAUSE_2,
                TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_0, dateTime2mins, 2, TEST_VALUE_TAC);
        insertData(true, TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_2,
                TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_0, dateTime3mins, 3, TEST_VALUE_TAC);
        insertData(true, TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_2,
                TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_0, dateTime48Hours, 3, TEST_VALUE_TAC);
        insertData(true, TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_0,
                TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_0, dateTime3mins, 3, SAMPLE_EXCLUSIVE_TAC);
        insertData(true, TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_2,
                TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_2, dateTime2mins, 1, TEST_VALUE_TAC);
        insertData(true, TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_2,
                TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_4, dateTime2mins, 1, TEST_VALUE_TAC);

        insertData(false, TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_2,
                TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_0, dateTime2mins, 1, TEST_VALUE_TAC);//should be in result
        insertData(false, TEST_VALUE_IMSI_2, TEST_VALUE_TBF_RELEASE_CAUSE_2,
                TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_0, dateTime2mins, 2, TEST_VALUE_TAC);//not expected in result - different imsi
        insertData(false, TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_2,
                TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_0, dateTime3mins, 3, TEST_VALUE_TAC);//should be in result
        insertData(false, TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_2,
                TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_0, dateTime48Hours, 9, TEST_VALUE_TAC); //not expected in result - outside time limit
        insertData(false, TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_2,
                TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_4, dateTime3mins, 3, SAMPLE_EXCLUSIVE_TAC);//should be in result - tacs not excluded for explicit imsi request
        insertData(false, TEST_VALUE_IMSI, TEST_VALUE_TBF_RELEASE_CAUSE_0,
                TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_3, dateTime2mins, 1, TEST_VALUE_TAC);//not expected in result - cause code not equal to 2
    }

    private void insertData(final boolean successEvents, final String imsi, final String causeCode,
            final String subCauseCode, final String time, final int instances, final int tac) throws SQLException {
        final Map<String, Object> dataForEventTable = new HashMap<String, Object>();
        if (successEvents) {
            dataForEventTable.put(NO_OF_SUCCESSES, instances);
            dataForEventTable.put(NO_OF_ERRORS, 0);
        } else {
            dataForEventTable.put(NO_OF_SUCCESSES, 0);
            dataForEventTable.put(NO_OF_ERRORS, instances);
        }
        dataForEventTable.put(TAC, tac);
        dataForEventTable.put(TBF_RELEASE_CAUSE, causeCode);//only Cause Code 2 should have a sub cause code
        dataForEventTable.put(CHANNEL_RELATED_RELEASE_CAUSE_GROUP, subCauseCode);
        dataForEventTable.put(DATETIME_ID, time);
        dataForEventTable.put(IMSI, imsi);
        insertRow(TEMP_EVENT_E_GSM_PS_ALL_RAW, dataForEventTable);
    }

    private void createLookupTables() throws Exception {
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_CHANNEL_RELATED_RELEASE_CAUSE_GROUP,
                "DIM_E_GSM_PS_CHANNEL_RELATED_RELEASE_CAUSE_GROUP", CHANNEL_RELATED_RELEASE_CAUSE_GROUP,
                CHANNEL_RELATED_RELEASE_CAUSE_GROUP_DESC);
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

    private void insertRowToUrgencyConditionTable(final String urgencyCondition,
            final String urgencyConditionDescription) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(CHANNEL_RELATED_RELEASE_CAUSE_GROUP, urgencyCondition);
        valuesForTable.put(CHANNEL_RELATED_RELEASE_CAUSE_GROUP_DESC, urgencyConditionDescription);
        insertRow(TEMP_DIM_E_GSM_PS_CHANNEL_RELATED_RELEASE_CAUSE_GROUP, valuesForTable);
    }

    private void insertUrgencyConditionLookupData() throws SQLException {
        insertRowToUrgencyConditionTable(TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_0,
                TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_0_DESC);
        insertRowToUrgencyConditionTable(TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_2,
                TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_2_DESC);
        insertRowToUrgencyConditionTable(TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_3,
                TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_3_DESC);
        insertRowToUrgencyConditionTable(TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_4,
                TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_4_DESC);
    }

    private void createEventTable() throws Exception {
        final Collection<String> columnsForEventTable = new ArrayList<String>();

        columnsForEventTable.add(NO_OF_ERRORS);
        columnsForEventTable.add(NO_OF_SUCCESSES);
        columnsForEventTable.add(TBF_RELEASE_CAUSE);
        columnsForEventTable.add(CHANNEL_RELATED_RELEASE_CAUSE_GROUP);
        columnsForEventTable.add(TAC);
        columnsForEventTable.add(IMSI);
        columnsForEventTable.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_PS_ALL_RAW, columnsForEventTable);
    }

}
