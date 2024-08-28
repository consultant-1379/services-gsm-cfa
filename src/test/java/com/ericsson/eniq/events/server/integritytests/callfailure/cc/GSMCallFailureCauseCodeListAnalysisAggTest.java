/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.callfailure.cc;

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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.cc.ControllerCCListService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.GSMCallFailureCauseCodeResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ewanggu
 * @since 2011
 *
 */

public class GSMCallFailureCauseCodeListAnalysisAggTest extends BaseDataIntegrityTest<GSMCallFailureCauseCodeResult> {

    private static final long TEST_VALUE_HIER3_ID = 5386564559998864911l;

    private static final String TEST_VALUE_CAUSE_GROUP_1 = "1";

    private static final String TEST_VALUE_CAUSE_GROUP_1_DESC = "EXCESSIVE TA";

    private static final String TEST_VALUE_CAUSE_GROUP_2 = "2";

    private static final String TEST_VALUE_CAUSE_GROUP_2_DESC = "SUDDENLY LOST CONNECTION";

    private static final String TEST_VALUE_CAUSE_GROUP_3 = "3";

    private static final String TEST_VALUE_CAUSE_GROUP_3_DESC = "LOW SS BOTHLINK";

    private static final String TEST_VALUE_VENDOR = "Ericsson";

    private String dateTime;

    private long bscHashId;

    private ControllerCCListService bscService;

    @Before
    public void setup() throws Exception {
        createHashId();
        createEventTable();
        createLookupTables();
        insertAllLookupData();
        insertEventData();
    }

    @Test
    public void testBscThreeDayQuery() throws URISyntaxException, Exception {
        bscService = new ControllerCCListService();
        attachDependencies(bscService);
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TIME_QUERY_PARAM, THREE_DAY);
        requestParameters.add(NODE_PARAM, TEST_VALUE_MSS_CONTROLLER_NODE);
        requestParameters.add(TYPE_PARAM, TYPE_BSC);
        final String result = runQuery(bscService, requestParameters);
        verifyResult(result);
    }

    private void verifyResult(final String json) throws Exception {
        assertJSONSucceeds(json);
        final List<GSMCallFailureCauseCodeResult> results = getTranslator().translateResult(json,
                GSMCallFailureCauseCodeResult.class);
        assertThat(bscHashId, is(TEST_VALUE_HIER3_ID));
        assertThat(results.size(), is(3));
        assertThat(results.get(0).getCauseCodeId(), is(TEST_VALUE_CAUSE_GROUP_1));
        assertThat(results.get(0).getCauseCodeDesc(), is(TEST_VALUE_CAUSE_GROUP_1_DESC));
        assertThat(results.get(1).getCauseCodeId(), is(TEST_VALUE_CAUSE_GROUP_2));
        assertThat(results.get(1).getCauseCodeDesc(), is(TEST_VALUE_CAUSE_GROUP_2_DESC));
        assertThat(results.get(2).getCauseCodeId(), is(TEST_VALUE_CAUSE_GROUP_3));
        assertThat(results.get(2).getCauseCodeDesc(), is(TEST_VALUE_CAUSE_GROUP_3_DESC));

    }

    private void createHashId() {
        bscHashId = queryUtils.createHashIDForController("GSM", TEST_VALUE_GSM_CONTROLLER1_NAME, TEST_VALUE_VENDOR);
    }

    private void insertEventData() throws Exception {
        dateTime = DateTimeUtilities.getDateMinus36Hours();
        insertData(TEST_VALUE_HIER3_ID, TEST_VALUE_CAUSE_GROUP_1, dateTime, 1);
        insertData(TEST_VALUE_HIER3_ID, TEST_VALUE_CAUSE_GROUP_2, dateTime, 1);
        insertData(TEST_VALUE_HIER3_ID, TEST_VALUE_CAUSE_GROUP_3, dateTime, 1);
    }

    private void insertData(final long hier3_id, final String causeCode, final String time, final int instances)
            throws SQLException {
        final Map<String, Object> dataForEventTable = new HashMap<String, Object>();
        dataForEventTable.clear();
        dataForEventTable.put(HIER3_ID, hier3_id);
        dataForEventTable.put(CAUSE_GROUP, causeCode);
        dataForEventTable.put(DATETIME_ID, time);
        dataForEventTable.put(NO_OF_ERRORS, instances);
        insertRow(TEMP_EVENT_E_GSM_CFA_HIER3_CG_ERR_15MIN, dataForEventTable);
    }

    private void createLookupTables() throws Exception {
        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_CAUSE_GROUP, "DIM_E_GSM_CFA_CAUSE_GROUP", CAUSE_GROUP,
                CAUSE_GROUP_DESC);
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
        valuesForTable.put(CAUSE_GROUP, urgencyCondition);
        valuesForTable.put(CAUSE_GROUP_DESC, urgencyConditionDescription);
        insertRow(TEMP_DIM_E_GSM_CFA_CAUSE_GROUP, valuesForTable);
    }

    private void insertUrgencyConditionLookupData() throws SQLException {
        insertRowToUrgencyConditionTable(TEST_VALUE_CAUSE_GROUP_1, TEST_VALUE_CAUSE_GROUP_1_DESC);
        insertRowToUrgencyConditionTable(TEST_VALUE_CAUSE_GROUP_2, TEST_VALUE_CAUSE_GROUP_2_DESC);
        insertRowToUrgencyConditionTable(TEST_VALUE_CAUSE_GROUP_3, TEST_VALUE_CAUSE_GROUP_3_DESC);
    }

    private void createEventTable() throws Exception {
        final Collection<String> columnsForEventTable = new ArrayList<String>();
        columnsForEventTable.clear();
        columnsForEventTable.add(HIER3_ID);
        columnsForEventTable.add(CAUSE_GROUP);
        columnsForEventTable.add(DATETIME_ID);
        columnsForEventTable.add(NO_OF_ERRORS);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_HIER3_CG_ERR_15MIN, columnsForEventTable);
    }

}
