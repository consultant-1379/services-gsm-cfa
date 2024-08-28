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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.AccessAreaSummaryService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.GSMAccessAreaCallFailureEventSummaryResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ehorpte
 * @since 2011
 *
 */
public class GSMCFACellEventSummaryRawTest extends BaseDataIntegrityTest<GSMAccessAreaCallFailureEventSummaryResult> {

    private AccessAreaSummaryService service;

    private long hashHier321Id;

    private long hashHier321IdIrrelevant1;

    private long hashHier321IdIrrelevant2;

    private long hashHier321IdIrrelevant3;

    private long hashHier321IdIrrelevant4;

    @Before
    public void onSetUp() throws Exception {
        createHashId();
        service = new AccessAreaSummaryService();
        attachDependencies(service);
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
    public void testFiveMinuteQueryNodeParam() throws URISyntaxException, Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        requestParameters.add(TIME_QUERY_PARAM, FIVE_MINUTES);
        requestParameters.add(NODE_PARAM, GSMCELL1 + ",," + BSC1 + "," + ERICSSON + "," + "GSM");
        requestParameters.add(TYPE_PARAM, TYPE_CELL);
        final String result = runQuery(service, requestParameters);
        verifyResult(result);
    }

    @Test
    public void testFiveMinuteQueryHashIdParam() throws URISyntaxException, Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        requestParameters.add(TIME_QUERY_PARAM, FIVE_MINUTES);
        requestParameters.add(CELL_SQL_ID, Long.toString(hashHier321Id));
        final String result = runQuery(service, requestParameters);
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
        assertThat(result.getNumFailures(), is(1));
        assertThat(result.getNumImpactedSubscribers(), is(1));
        assertThat(result.getCategoryId(), is(1));
        assertThat(result.getHier321Id(), is(hashHier321Id));
        assertThat(result.getAccessArea(), is(GSMCELL1));
        assertThat(result.getRatio(), is(25.00));

        result = results.get(1);

        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(BSC1));
        assertThat(result.getEventDescription(), is("Call Drop"));
        assertThat(result.getNumFailures(), is(1));
        assertThat(result.getNumImpactedSubscribers(), is(1));
        assertThat(result.getCategoryId(), is(0));
        assertThat(result.getHier321Id(), is(hashHier321Id));
        assertThat(result.getAccessArea(), is(GSMCELL1));
        assertThat(result.getRatio(), is(25.00));

    }

    private void insertData() throws Exception {
        final Map<String, Object> columnsFor_DIM_E_SGEH_HIER321 = new HashMap<String, Object>();
        columnsFor_DIM_E_SGEH_HIER321.put(HIERARCHY_3, BSC1);
        columnsFor_DIM_E_SGEH_HIER321.put(HIERARCHY_1, GSMCELL1);
        columnsFor_DIM_E_SGEH_HIER321.put(HIER321_ID, hashHier321Id);
        columnsFor_DIM_E_SGEH_HIER321.put(VENDOR, ERICSSON);
        columnsFor_DIM_E_SGEH_HIER321.put(RAT, RAT_INTEGER_VALUE_FOR_2G);
        insertRow(TEMP_DIM_E_SGEH_HIER321, columnsFor_DIM_E_SGEH_HIER321);

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

        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(HIER321_ID, hashHier321Id);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(EVENT_ID, GSM_CALL_DROP_EVENT_ID_AS_INTEGER);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(IMSI, 1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinus5Minutes());
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);

        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.clear();

        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(HIER321_ID, hashHier321Id);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(EVENT_ID, GSM_CALL_DROP_EVENT_ID_AS_INTEGER);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(IMSI, 1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinus5Minutes());
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);

        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(HIER321_ID, hashHier321Id);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(EVENT_ID, GSM_CALL_DROP_EVENT_ID_AS_INTEGER);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(IMSI, 1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(DATETIME_ID,
                DateTimeUtilities.getDateTimeMinus5Minutes());
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC);

        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1 = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1.put(HIER321_ID, hashHier321IdIrrelevant1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1.put(EVENT_ID, GSM_CALL_DROP_EVENT_ID_AS_INTEGER);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1.put(IMSI, 1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1.put(DATETIME_ID,
                DateTimeUtilities.getDateTimeMinus5Minutes());
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1.put(CATEGORY_ID,
                GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1);

        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2 = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2.put(HIER321_ID, hashHier321IdIrrelevant2);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2.put(EVENT_ID, GSM_CALL_DROP_EVENT_ID_AS_INTEGER);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2.put(IMSI, 1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2.put(DATETIME_ID,
                DateTimeUtilities.getDateTimeMinus5Minutes());
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2.put(CATEGORY_ID,
                GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_2);

        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_3 = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_3.put(HIER321_ID, hashHier321IdIrrelevant3);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_3.put(EVENT_ID, GSM_CALL_DROP_EVENT_ID_AS_INTEGER);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_3.put(IMSI, 1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_3.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_3.put(DATETIME_ID,
                DateTimeUtilities.getDateTimeMinus5Minutes());
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_3.put(CATEGORY_ID,
                GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_3);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_3);

        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_4 = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_4.put(HIER321_ID, hashHier321IdIrrelevant4);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_4.put(EVENT_ID, GSM_CALL_DROP_EVENT_ID_AS_INTEGER);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_4.put(IMSI, 1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_4.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_4.put(DATETIME_ID,
                DateTimeUtilities.getDateTimeMinus5Minutes());
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_4.put(CATEGORY_ID,
                GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_4);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_4);

        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_DATETIME_ID = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_DATETIME_ID.put(HIER321_ID, hashHier321Id);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_DATETIME_ID.put(EVENT_ID, GSM_CALL_DROP_EVENT_ID_AS_INTEGER);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_DATETIME_ID.put(IMSI, 1);
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
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.add(EVENT_ID_DESC);
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.add(EVENT_ID);
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.add(CATEGORY_ID_DESC);
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.add(CATEGORY_ID);
        createTemporaryTable(TEMP_DIM_E_GSM_CFA_EVENTTYPE, columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE);

        final Collection<String> columnsFor_DIM_E_SGEH_HIER321 = new ArrayList<String>();
        columnsFor_DIM_E_SGEH_HIER321.add(HIERARCHY_3);
        columnsFor_DIM_E_SGEH_HIER321.add(HIERARCHY_1);
        columnsFor_DIM_E_SGEH_HIER321.add(HIER321_ID);
        columnsFor_DIM_E_SGEH_HIER321.add(VENDOR);
        columnsFor_DIM_E_SGEH_HIER321.add(RAT);
        createTemporaryTable(TEMP_DIM_E_SGEH_HIER321, columnsFor_DIM_E_SGEH_HIER321);

        final Collection<String> columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW = new ArrayList<String>();
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(HIER321_ID);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(EVENT_ID);
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

    private void insertDataIntoTacGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }
}
