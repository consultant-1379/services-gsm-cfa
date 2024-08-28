/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.callfailure.roaming;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.roaming.OperatorDrillRoamingAnalysisService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.OperatorDrillRoamingAnalysisQueryResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author eprjaya
 *
 */
public class OperatorDrillRoamingServiceAggTest extends BaseDataIntegrityTest<OperatorDrillRoamingAnalysisQueryResult> {

    private OperatorDrillRoamingAnalysisService OperatorDrillRoamingAnalysisService;

    private static final String SAMPLE_MCC = "710";

    private static final String SAMPLE_MNC = "010";

    @Before
    public void setup() throws Exception {
        OperatorDrillRoamingAnalysisService = new OperatorDrillRoamingAnalysisService();
        attachDependencies(OperatorDrillRoamingAnalysisService);
        createTables();
        createLookupTables();
        insertData();
        insertAllLookupData();
    }

    private void insertData() throws Exception {

        insertIntoRawTable();

        insertSuccessData();

        final String dateTime = DateTimeUtilities.getDateTimeMinusHours(1);

        insertRowsIntoAggregationView(SAMPLE_MCC, SAMPLE_MNC, dateTime, 3);
        insertRowsIntoAggregationView(SAMPLE_MCC, SAMPLE_MNC, dateTime, 3);

    }

    private void insertIntoRawTable() throws SQLException {
        final String timestamp = DateTimeUtilities.getDateTimeMinusHours(1);

        insertRowIntoRawTable(1, SAMPLE_MCC, SAMPLE_MNC, 1, timestamp, 1);
        insertRowIntoRawTable(2, SAMPLE_MCC, SAMPLE_MNC, 1, timestamp, 1);
        insertRowIntoRawTable(2, SAMPLE_MCC, SAMPLE_MNC, 1, timestamp, 1);
        insertRowIntoRawTable(3, SAMPLE_MCC, SAMPLE_MNC, 1, timestamp, 0);
        insertRowIntoRawTable(4, SAMPLE_MCC, SAMPLE_MNC, 1, timestamp, 0);
        insertRowIntoRawTable(4, SAMPLE_MCC, SAMPLE_MNC, 0, timestamp, 0);

        //this event shouldn't be included in the result - per MZ, if the IMSI_MCC cannot be determined,
        //the roaming column will never be set to 1
        insertRowIntoRawTable(SAMPLE_IMSI, null, SAMPLE_MNC, 0, timestamp, 1);

    }

    private void insertRowsIntoAggregationView(final String mcc, final String mnc, final String datetime,
            final int instances) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(IMSI_MCC, mcc);
        valuesForTable.put(IMSI_MNC, mnc);
        valuesForTable.put(NO_OF_ERRORS, instances);
        valuesForTable.put(DATETIME_ID, datetime);
        valuesForTable.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_MCC_MNC_ROAM_15MIN, valuesForTable);

        valuesForTable.put(CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID_AS_INTEGER);
        insertRow(TEMP_EVENT_E_GSM_CFA_MCC_MNC_ROAM_15MIN, valuesForTable);
    }

    private void insertSuccessData() throws Exception {
        final Map<String, Object> successData = new HashMap<String, Object>();
        //insert 10 successful calls for this BSC
        successData.put(IMSI_MCC, SAMPLE_MCC);
        successData.put(IMSI_MNC, SAMPLE_MNC);
        successData.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusHours(1));
        successData.put(NO_OF_SUCCESSES, 10);
        insertRow(TEMP_EVENT_E_GSM_CFA_MCC_MNC_ROAM_SUC_15MIN, successData);

    }

    private void insertRowIntoRawTable(final long imsi, final String imsi_mcc, final String imsi_mnc,
            final int roamingValue, final String timestamp, final int categoryID) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(IMSI, imsi);
        values.put(IMSI_MCC, imsi_mcc);
        values.put(IMSI_MNC, imsi_mnc);
        values.put(ROAMING, roamingValue);
        values.put(TAC, 12312);
        values.put(DATETIME_ID, timestamp);
        values.put(CATEGORY_ID, categoryID);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, values);
    }

    private void createTables() throws Exception {
        final Collection<String> columnsForEventTable = new ArrayList<String>();
        columnsForEventTable.add(IMSI_MCC);
        columnsForEventTable.add(IMSI_MNC);
        columnsForEventTable.add(NO_OF_ERRORS);
        columnsForEventTable.add(CATEGORY_ID);
        columnsForEventTable.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_MCC_MNC_ROAM_15MIN, columnsForEventTable);

        columnsForEventTable.clear();
        columnsForEventTable.add(IMSI_MCC);
        columnsForEventTable.add(IMSI_MNC);
        columnsForEventTable.add(DATETIME_ID);
        columnsForEventTable.add(NO_OF_SUCCESSES);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_MCC_MNC_ROAM_SUC_15MIN, columnsForEventTable);

        final Collection<String> columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW = new ArrayList<String>();
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(IMSI);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(DATETIME_ID);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(CATEGORY_ID);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(IMSI_MCC);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(IMSI_MNC);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(ROAMING);
        columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW.add(TAC);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columnsFor_TEMP_EVENT_E_GSM_CFA_ERR_RAW);

        ReplaceTablesWithTempTablesTemplateUtils.addTableNameToReplace("EVENT_E_GSM_CFA_MCC_MNC_ROAM_15MIN",
                TEMP_EVENT_E_GSM_CFA_MCC_MNC_ROAM_15MIN);
        ReplaceTablesWithTempTablesTemplateUtils.addTableNameToReplace("EVENT_E_GSM_CFA_MCC_MNC_ROAM_SUC_15MIN",
                TEMP_EVENT_E_GSM_CFA_MCC_MNC_ROAM_SUC_15MIN);
        ReplaceTablesWithTempTablesTemplateUtils.addTableNameToReplace("EVENT_E_GSM_CFA_ERR_RAW",
                TEMP_EVENT_E_GSM_CFA_ERR_RAW);
    }

    private void createLookupTables() throws Exception {

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_CFA_EVENTTYPE, "DIM_E_GSM_CFA_EVENTTYPE", CATEGORY_ID,
                CATEGORY_ID_DESC);
        createAndReplaceLookupTable(TEMP_DIM_E_SGEH_MCCMNC, "DIM_E_SGEH_MCCMNC", COUNTRY, OPERATOR, MCC, MNC);
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
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(COUNTRY, "ARGENTINA");
        valuesForTable.put(OPERATOR, MOVISTAR);
        valuesForTable.put(MCC, SAMPLE_MCC);
        valuesForTable.put(MNC, SAMPLE_MNC);
        insertRow(TEMP_DIM_E_SGEH_MCCMNC, valuesForTable);

        valuesForTable.clear();

        valuesForTable.put(CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID);
        valuesForTable.put(CATEGORY_ID_DESC, "Call Setup Failures");
        insertRow(TEMP_DIM_E_GSM_CFA_EVENTTYPE, valuesForTable);

        valuesForTable.clear();

        valuesForTable.put(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID);
        valuesForTable.put(CATEGORY_ID_DESC, "Call Drops");
        insertRow(TEMP_DIM_E_GSM_CFA_EVENTTYPE, valuesForTable);

        valuesForTable.clear();
    }

    @Test
    public void testGetCountryRoamingData_Agg() throws Exception {
        final MultivaluedMap<String, String> parameters = new MultivaluedMapImpl();
        parameters.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        parameters.putSingle(TIME_QUERY_PARAM, ONE_DAY);
        parameters.putSingle(MCC, SAMPLE_MCC);
        parameters.putSingle(MNC, SAMPLE_MNC);
        final String result = runQuery(OperatorDrillRoamingAnalysisService, parameters);
        validateResult(result);
    }

    private void validateResult(final String json) throws Exception {
        final List<OperatorDrillRoamingAnalysisQueryResult> results = getTranslator().translateResult(json,
                OperatorDrillRoamingAnalysisQueryResult.class);
        assertThat(results.size(), is(2));
        validateAgainstGridDefinition(json, "GSM_ROAMING_ANALYSIS_DRILL_BY_OPERATOR");

        OperatorDrillRoamingAnalysisQueryResult result = results.get(0);

        assertThat(result.getOperatorName(), is(MOVISTAR));
        assertThat(result.getNumErrors(), is(6));
        assertThat(result.getImpactedSubscribers(), is(2));
        assertThat(result.getCategoryIDDesc(), is("Call Setup Failures"));
        assertThat(result.getCategoryID(), is(1));
        assertThat(result.getFailureRatio(), is(60.0));
        assertThat(result.getMcc(), is("710"));
        assertThat(result.getMnc(), is("010"));

        result = results.get(1);

        assertThat(result.getOperatorName(), is(MOVISTAR));
        assertThat(result.getNumErrors(), is(6));
        assertThat(result.getImpactedSubscribers(), is(2));
        assertThat(result.getCategoryIDDesc(), is("Call Drops"));
        assertThat(result.getCategoryID(), is(0));
        assertThat(result.getFailureRatio(), is(60.0));
        assertThat(result.getMcc(), is("710"));
        assertThat(result.getMnc(), is("010"));
    }
}
