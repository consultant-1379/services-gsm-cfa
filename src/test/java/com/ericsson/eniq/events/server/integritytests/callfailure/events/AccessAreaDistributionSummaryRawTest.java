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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events.AccessAreaDistributionSummaryService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.DistributionCellEventSummaryResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ewanggu
 * @since 2012
 *
 */
public class AccessAreaDistributionSummaryRawTest extends BaseDataIntegrityTest<DistributionCellEventSummaryResult> {

    private AccessAreaDistributionSummaryService service;

    private String hashHier3Id = "";

    private String hashHier321Id1 = "";

    private String hashHier321Id2 = "";

    private String hashHier321IdIrrelevant1 = "";

    private String hashHier321IdIrrelevant2 = "";

    private String hashHier3IdIrrelevant1 = "";

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
        hashHier321Id1 = queryUtils.createHashIDForCell(RAT_INTEGER_VALUE_FOR_2G, BSC1, "", GSMCELL2, ERICSSON) + "";
        hashHier321Id2 = queryUtils.createHashIDForCell(RAT_INTEGER_VALUE_FOR_2G, BSC1, "", GSMCELL1, ERICSSON) + "";
        hashHier3IdIrrelevant1 = queryUtils.createHashIDForController(RAT_INTEGER_VALUE_FOR_2G, BSC2, ERICSSON) + "";
        hashHier321IdIrrelevant1 = queryUtils.createHashIDForCell(RAT_INTEGER_VALUE_FOR_2G, BSC2, "", GSMCELL1,
                ERICSSON) + "";
        hashHier321IdIrrelevant2 = queryUtils.createHashIDForCell(RAT_INTEGER_VALUE_FOR_2G, BSC1, "", GSMCELL2,
                SONY_ERICSSON) + "";

    }

    @Test
    public void testFiveMinuteQueryHashIdParam() throws URISyntaxException, Exception {
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        requestParameters.add(TIME_QUERY_PARAM, FIVE_MINUTES);
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
            assertThat(result.getCategoryId(), is(0));
            assertThat(result.getRatio(), is(50.00));
        }
        assertThat(results.get(0).getNumFailures(), is(2));
        assertThat(results.get(0).getNumImpactedSubscribers(), is(2));
        assertThat(results.get(1).getNumFailures(), is(1));
        assertThat(results.get(1).getNumImpactedSubscribers(), is(1));
        assertThat(results.get(0).getAccessArea(), is(GSMCELL2));
        assertThat(results.get(1).getAccessArea(), is(GSMCELL1));
    }

    private void insertData() throws Exception {
        final Map<String, Object> columnsFor_DIM_E_SGEH_HIER321 = new HashMap<String, Object>();
        columnsFor_DIM_E_SGEH_HIER321.put(HIERARCHY_3, BSC1);
        columnsFor_DIM_E_SGEH_HIER321.put(HIERARCHY_1, GSMCELL1);
        columnsFor_DIM_E_SGEH_HIER321.put(HIER321_ID, hashHier321Id1);
        columnsFor_DIM_E_SGEH_HIER321.put(HIER3_ID, hashHier3Id);
        columnsFor_DIM_E_SGEH_HIER321.put(VENDOR, ERICSSON);
        columnsFor_DIM_E_SGEH_HIER321.put(RAT, RAT_INTEGER_VALUE_FOR_2G);
        insertRow(TEMP_DIM_E_SGEH_HIER321, columnsFor_DIM_E_SGEH_HIER321);

        columnsFor_DIM_E_SGEH_HIER321.clear();
        columnsFor_DIM_E_SGEH_HIER321.put(HIERARCHY_3, BSC1);
        columnsFor_DIM_E_SGEH_HIER321.put(HIERARCHY_1, GSMCELL2);
        columnsFor_DIM_E_SGEH_HIER321.put(HIER321_ID, hashHier321Id2);
        columnsFor_DIM_E_SGEH_HIER321.put(HIER3_ID, hashHier3Id);
        columnsFor_DIM_E_SGEH_HIER321.put(VENDOR, ERICSSON);
        columnsFor_DIM_E_SGEH_HIER321.put(RAT, RAT_INTEGER_VALUE_FOR_2G);
        insertRow(TEMP_DIM_E_SGEH_HIER321, columnsFor_DIM_E_SGEH_HIER321);

        columnsFor_DIM_E_SGEH_HIER321.clear();
        columnsFor_DIM_E_SGEH_HIER321.put(HIERARCHY_3, BSC2);
        columnsFor_DIM_E_SGEH_HIER321.put(HIERARCHY_1, GSMCELL1);
        columnsFor_DIM_E_SGEH_HIER321.put(HIER321_ID, hashHier321IdIrrelevant1);
        columnsFor_DIM_E_SGEH_HIER321.put(HIER3_ID, hashHier3IdIrrelevant1);
        columnsFor_DIM_E_SGEH_HIER321.put(VENDOR, ERICSSON);
        columnsFor_DIM_E_SGEH_HIER321.put(RAT, RAT_INTEGER_VALUE_FOR_2G);
        insertRow(TEMP_DIM_E_SGEH_HIER321, columnsFor_DIM_E_SGEH_HIER321);

        columnsFor_DIM_E_SGEH_HIER321.clear();
        columnsFor_DIM_E_SGEH_HIER321.put(HIERARCHY_3, BSC2);
        columnsFor_DIM_E_SGEH_HIER321.put(HIERARCHY_1, GSMCELL2);
        columnsFor_DIM_E_SGEH_HIER321.put(HIER321_ID, hashHier321IdIrrelevant1);
        columnsFor_DIM_E_SGEH_HIER321.put(HIER3_ID, hashHier3IdIrrelevant1);
        columnsFor_DIM_E_SGEH_HIER321.put(VENDOR, ERICSSON);
        columnsFor_DIM_E_SGEH_HIER321.put(RAT, RAT_INTEGER_VALUE_FOR_2G);
        insertRow(TEMP_DIM_E_SGEH_HIER321, columnsFor_DIM_E_SGEH_HIER321);

        final Map<String, Object> columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE = new HashMap<String, Object>();
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.put(CATEGORY_ID_DESC, "Call Drop");
        insertRow(TEMP_DIM_E_GSM_CFA_EVENTTYPE, columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE);

        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(HIER321_ID, hashHier321Id1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(HIER3_ID, hashHier3Id);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(IMSI, 1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinus2Minutes());
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);

        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.clear();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(HIER321_ID, hashHier321Id2);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(HIER3_ID, hashHier3Id);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(IMSI, 2);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinus2Minutes());
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);

        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(HIER321_ID, hashHier321Id1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(HIER3_ID, hashHier3Id);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(IMSI, 1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(DATETIME_ID,
                DateTimeUtilities.getDateTimeMinus2Minutes());
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Exclusive_TAC);

        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.clear();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(HIER321_ID, hashHier321Id2);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(HIER3_ID, hashHier3Id);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(IMSI, 1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinus2Minutes());
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);

        final Map<String, Object> dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1 = new HashMap<String, Object>();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1.put(HIER321_ID, hashHier321IdIrrelevant1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1.put(HIER3_ID, hashHier3IdIrrelevant1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1.put(IMSI, 1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1.put(DATETIME_ID,
                DateTimeUtilities.getDateTimeMinus2Minutes());
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1.put(CATEGORY_ID,
                GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1);

        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1.clear();
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1.put(HIER321_ID, hashHier321IdIrrelevant2);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1.put(HIER3_ID, hashHier3IdIrrelevant1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1.put(IMSI, 1);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1.put(TAC, SAMPLE_TAC);
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1.put(DATETIME_ID,
                DateTimeUtilities.getDateTimeMinus5Minutes());
        dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1.put(CATEGORY_ID,
                GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, dataFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW_Irrelevant_HASH321_ID_1);
    }

    private void createTables() throws Exception {
        final Collection<String> columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE = new ArrayList<String>();
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.add(CATEGORY_ID_DESC);
        columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE.add(CATEGORY_ID);
        createTemporaryTable(TEMP_DIM_E_GSM_CFA_EVENTTYPE, columnsFor_TEMP_DIM_E_GSM_CFA_EVENTTYPE);

        final Collection<String> columnsFor_DIM_E_SGEH_HIER321 = new ArrayList<String>();
        columnsFor_DIM_E_SGEH_HIER321.add(HIERARCHY_3);
        columnsFor_DIM_E_SGEH_HIER321.add(HIERARCHY_1);
        columnsFor_DIM_E_SGEH_HIER321.add(HIER321_ID);
        columnsFor_DIM_E_SGEH_HIER321.add(HIER3_ID);
        columnsFor_DIM_E_SGEH_HIER321.add(VENDOR);
        columnsFor_DIM_E_SGEH_HIER321.add(RAT);
        createTemporaryTable(TEMP_DIM_E_SGEH_HIER321, columnsFor_DIM_E_SGEH_HIER321);

        final Collection<String> columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW = new ArrayList<String>();
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(HIER321_ID);
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

    private void insertDataIntoTacGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }
}
