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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.SubscriberSummaryService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.CFAImsiEventSummaryResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author eramiye
 * @since 2012
 *
 */

public class CFAImsiEventSummaryRawTest extends BaseDataIntegrityTest<CFAImsiEventSummaryResult> {

    private SubscriberSummaryService gsmCFAImsiEventSummaryService;

    @Before
    public void onSetUp() throws Exception {
        gsmCFAImsiEventSummaryService = new SubscriberSummaryService();
        attachDependencies(gsmCFAImsiEventSummaryService);
        createTables();
        insertDataIntoTacGroupTable();
        insertData();
    }

    @Test
    public void testFiveMinuteQuery() throws URISyntaxException, Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        requestParameters.add(TIME_QUERY_PARAM, FIVE_MINUTES);
        requestParameters.add(TYPE_PARAM, TYPE_IMSI);
        requestParameters.add(CATEGORY_ID, "0");
        requestParameters.add(IMSI_PARAM, "123456789123456");
        requestParameters.add(DISPLAY_PARAM, GRID_PARAM);

        final String result = runQuery(gsmCFAImsiEventSummaryService, requestParameters);
        verifyResult(result);
    }

    private void verifyResult(final String json) throws Exception {
        assertJSONSucceeds(json);
        final List<CFAImsiEventSummaryResult> results = getTranslator().translateResult(json,
                CFAImsiEventSummaryResult.class);

        assertThat(results.size(), is(2));
        validateAgainstGridDefinition(json, "GSM_SUBSCRIBER_SUMMARY_EVENT_ANALYSIS_BY_IMSI");
        final CFAImsiEventSummaryResult csf_result = results.get(0);
        final CFAImsiEventSummaryResult cd_result = results.get(1);

        assertThat(cd_result.getCategoryDescription(), is(GSM_CALL_DROP_CATEGORY_ID_DESC));
        assertThat(cd_result.getNumberOfFailures(), is(4));
        assertThat(cd_result.getRatio(), is(25.0));
        assertThat(cd_result.getCategoryId(), is(0));
        assertThat(cd_result.getImsi(), is("123456789123456"));

        assertThat(csf_result.getCategoryDescription(), is(GSM_CALL_SETUP_FAILURES_CATEGORY_ID_DESC));
        assertThat(csf_result.getNumberOfFailures(), is(4));
        assertThat(csf_result.getRatio(), is(25.0));
        assertThat(csf_result.getCategoryId(), is(1));
        assertThat(csf_result.getImsi(), is("123456789123456"));
    }

    private void insertDataIntoTacGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    private void insertData() throws Exception {
        final String time = DateTimeUtilities.getDateTimeMinus2Minutes();
        final Map<String, Object> columnsFor_DIM_E_SGEH_HIER321_CELL = new HashMap<String, Object>();
        columnsFor_DIM_E_SGEH_HIER321_CELL.put(HIERARCHY_3, BSC1);
        columnsFor_DIM_E_SGEH_HIER321_CELL.put(HIER3_ID, TEST_VALUE_GSM_HIER3_ID_BSC1);
        columnsFor_DIM_E_SGEH_HIER321_CELL.put(VENDOR, ERICSSON);
        columnsFor_DIM_E_SGEH_HIER321_CELL.put(RAT, RAT_INTEGER_VALUE_FOR_2G);
        insertRow(TEMP_DIM_E_SGEH_HIER321, columnsFor_DIM_E_SGEH_HIER321_CELL);

        final Map<String, Object> columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE = new HashMap<String, Object>();
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.put(CATEGORY_ID_DESC, GSM_CALL_DROP_CATEGORY_ID_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_EVENTTYPE, columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE);

        final Map<String, Object> columnsFor_CSF_TEMP_DIM_E_GSM_CFA_EVENTTYPE = new HashMap<String, Object>();
        columnsFor_CSF_TEMP_DIM_E_GSM_CFA_EVENTTYPE.put(CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER);
        columnsFor_CSF_TEMP_DIM_E_GSM_CFA_EVENTTYPE.put(CATEGORY_ID_DESC, GSM_CALL_SETUP_FAILURES_CATEGORY_ID_DESC);
        insertRow(TEMP_DIM_E_GSM_CFA_EVENTTYPE, columnsFor_CSF_TEMP_DIM_E_GSM_CFA_EVENTTYPE);

        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(HIER3_ID, TEST_VALUE_GSM_HIER3_ID_BSC1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(IMSI, "123456789123456");
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(DATETIME_ID, time);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);

        final Map<String, Object> dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW = new HashMap<String, Object>();
        dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(HIER3_ID, TEST_VALUE_GSM_HIER3_ID_BSC1);
        dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(IMSI, "123456789123456");
        dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(TAC, SAMPLE_TAC);
        dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(DATETIME_ID, time);
        dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW);

        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(HIER3_ID, TEST_VALUE_GSM_HIER3_ID_BSC1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(IMSI, "123456789123456");
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(DATETIME_ID, time);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC);

        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_1 = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_1.put(HIER3_ID, TEST_VALUE_GSM_HIER3_ID_BSC1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_1.put(IMSI, "98765432109876");
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_1.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_1.put(DATETIME_ID, time);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_1.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_1);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_1);

        final Map<String, Object> dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_1 = new HashMap<String, Object>();
        dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_1.put(HIER3_ID, TEST_VALUE_GSM_HIER3_ID_BSC1);
        dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_1.put(IMSI, "98765432109876");
        dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_1.put(TAC, SAMPLE_TAC);
        dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_1.put(DATETIME_ID, time);
        dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_1.put(CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_1);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_1);

        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_2 = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_2.put(HIER3_ID, TEST_VALUE_GSM_HIER3_ID_BSC1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_2.put(IMSI, "123456789123456");
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_2.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_2.put(DATETIME_ID, time);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_2.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_2);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_2);

        final Map<String, Object> dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_2 = new HashMap<String, Object>();
        dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_2.put(HIER3_ID, TEST_VALUE_GSM_HIER3_ID_BSC1);
        dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_2.put(IMSI, "123456789123456");
        dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_2.put(TAC, SAMPLE_TAC);
        dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_2.put(DATETIME_ID, time);
        dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_2.put(CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_2);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_2);

        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_3 = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_3.put(HIER3_ID, TEST_VALUE_GSM_HIER3_ID_BSC1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_3.put(IMSI, "123456789123456");
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_3.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_3.put(DATETIME_ID, time);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_3.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_3);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_3);

        final Map<String, Object> dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_3 = new HashMap<String, Object>();
        dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_3.put(HIER3_ID, TEST_VALUE_GSM_HIER3_ID_BSC1);
        dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_3.put(IMSI, "123456789123456");
        dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_3.put(TAC, SAMPLE_TAC);
        dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_3.put(DATETIME_ID, time);
        dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_3.put(CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_3);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_3);

        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_4 = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_4.put(HIER3_ID, TEST_VALUE_GSM_HIER3_ID_BSC1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_4.put(IMSI, "123456789123456");
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_4.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_4.put(DATETIME_ID, time);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_4.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_4);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_4);

        final Map<String, Object> dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_4 = new HashMap<String, Object>();
        dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_4.put(HIER3_ID, TEST_VALUE_GSM_HIER3_ID_BSC1);
        dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_4.put(IMSI, "123456789123456");
        dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_4.put(TAC, SAMPLE_TAC);
        dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_4.put(DATETIME_ID, time);
        dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_4.put(CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_4);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataFor_CSF_TEMP_EVENT_E_GSM_CFA_ERR_RAW_4);

        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_DATETIME_ID = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_DATETIME_ID.put(HIER3_ID, TEST_VALUE_GSM_HIER3_ID_BSC1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_DATETIME_ID.put(IMSI, "123456789123456");
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_DATETIME_ID.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_DATETIME_ID.put(DATETIME_ID,
                DateTimeUtilities.getDateTimeMinus48Hours());
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_DATETIME_ID.put(CATEGORY_ID,
                GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_DATETIME_ID);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_DATETIME_ID);

    }

    private void createTables() throws Exception {
        final Collection<String> columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE = new ArrayList<String>();
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.add(CATEGORY_ID_DESC);
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.add(CATEGORY_ID);
        createTemporaryTable(TEMP_DIM_E_GSM_CFA_EVENTTYPE, columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE);

        final Collection<String> columnsFor_DIM_E_SGEH_HIER321 = new ArrayList<String>();
        columnsFor_DIM_E_SGEH_HIER321.add(HIER3_ID);
        columnsFor_DIM_E_SGEH_HIER321.add(HIERARCHY_3);
        columnsFor_DIM_E_SGEH_HIER321.add(VENDOR);
        columnsFor_DIM_E_SGEH_HIER321.add(RAT);
        createTemporaryTable(TEMP_DIM_E_SGEH_HIER321, columnsFor_DIM_E_SGEH_HIER321);

        final Collection<String> columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW = new ArrayList<String>();
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(HIER3_ID);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(IMSI);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(TAC);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(DATETIME_ID);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(CATEGORY_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_SUC_RAW, columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);

        ReplaceTablesWithTempTablesTemplateUtils.addTableNameToReplace("DIM_E_SGEH_HIER321", TEMP_DIM_E_SGEH_HIER321);
        ReplaceTablesWithTempTablesTemplateUtils.addTableNameToReplace("DIM_E_GSM_CFA_EVENTTYPE",
                TEMP_DIM_E_GSM_CFA_EVENTTYPE);
    }
}
