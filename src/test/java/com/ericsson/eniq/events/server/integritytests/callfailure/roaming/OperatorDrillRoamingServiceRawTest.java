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
public class OperatorDrillRoamingServiceRawTest extends BaseDataIntegrityTest<OperatorDrillRoamingAnalysisQueryResult> {

    private OperatorDrillRoamingAnalysisService OperatorDrillRoamingAnalysisService;

    private static final String SAMPLE_MNC = "010";

    @Before
    public void setup() throws Exception {
        OperatorDrillRoamingAnalysisService = new OperatorDrillRoamingAnalysisService();
        attachDependencies(OperatorDrillRoamingAnalysisService);
        createRawTable();
        createSucTable();
        insertRawData();
        insertSucData();
        createLookupTables();
        insertAllLookupData();
    }

    private void insertRawData() throws SQLException {
        final String timestamp = DateTimeUtilities.getDateTimeMinus2Minutes();

        insertRowIntoRawTable(1, MCC_FOR_ARGENTINA, SAMPLE_MNC, 1, timestamp, 1);
        insertRowIntoRawTable(2, MCC_FOR_ARGENTINA, SAMPLE_MNC, 1, timestamp, 1);
        insertRowIntoRawTable(2, MCC_FOR_ARGENTINA, SAMPLE_MNC, 1, timestamp, 1);
        insertRowIntoRawTable(3, MCC_FOR_ARGENTINA, SAMPLE_MNC, 1, timestamp, 0);
        insertRowIntoRawTable(4, MCC_FOR_ARGENTINA, SAMPLE_MNC, 1, timestamp, 0);
        insertRowIntoRawTable(4, MCC_FOR_ARGENTINA, SAMPLE_MNC, 0, timestamp, 0);

        //this event shouldn't be included in the result - per MZ, if the IMSI_MCC cannot be determined,
        //the roaming column will never be set to 1
        insertRowIntoRawTable(SAMPLE_IMSI, null, SAMPLE_MNC, 0, timestamp, 1);

    }

    private void insertSucData() throws SQLException {
        final String timestamp = DateTimeUtilities.getDateTimeMinus2Minutes();

        insertRowIntoSucTable(1, MCC_FOR_ARGENTINA, SAMPLE_MNC, 1, timestamp, 1);
        insertRowIntoSucTable(2, MCC_FOR_ARGENTINA, SAMPLE_MNC, 1, timestamp, 1);
        insertRowIntoSucTable(2, MCC_FOR_ARGENTINA, SAMPLE_MNC, 1, timestamp, 1);
        insertRowIntoSucTable(3, MCC_FOR_ARGENTINA, SAMPLE_MNC, 1, timestamp, 0);
        insertRowIntoSucTable(4, MCC_FOR_ARGENTINA, SAMPLE_MNC, 1, timestamp, 0);
        insertRowIntoSucTable(4, MCC_FOR_ARGENTINA, SAMPLE_MNC, 0, timestamp, 0);

        //this event shouldn't be included in the result - per MZ, if the IMSI_MCC cannot be determined,
        //the roaming column will never be set to 1
        insertRowIntoSucTable(SAMPLE_IMSI, null, SAMPLE_MNC, 0, timestamp, 1);

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

    private void insertRowIntoSucTable(final long imsi, final String imsi_mcc, final String imsi_mnc,
            final int roamingValue, final String timestamp, final int categoryID) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(IMSI, imsi);
        values.put(IMSI_MCC, imsi_mcc);
        values.put(IMSI_MNC, imsi_mnc);
        values.put(ROAMING, roamingValue);
        values.put(TAC, 12312);
        values.put(DATETIME_ID, timestamp);
        values.put(CATEGORY_ID, categoryID);
        insertRow(TEMP_EVENT_E_GSM_CFA_SUC_RAW, values);
    }

    private void createRawTable() throws Exception {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(IMSI);
        columns.add(CATEGORY_ID);
        columns.add(IMSI_MCC);
        columns.add(IMSI_MNC);
        columns.add(ROAMING);
        columns.add(TAC);
        columns.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columns);
    }

    private void createSucTable() throws Exception {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(IMSI);
        columns.add(CATEGORY_ID);
        columns.add(IMSI_MCC);
        columns.add(IMSI_MNC);
        columns.add(ROAMING);
        columns.add(TAC);
        columns.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_SUC_RAW, columns);
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
        valuesForTable.put(MCC, MCC_FOR_ARGENTINA);
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
    public void testGetCountryRoamingData_5Minutes() throws Exception {
        final MultivaluedMap<String, String> parameters = new MultivaluedMapImpl();
        parameters.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        parameters.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        parameters.putSingle(MCC, MCC_FOR_ARGENTINA);
        parameters.putSingle(MNC, SAMPLE_MNC);
        final String result = runQuery(OperatorDrillRoamingAnalysisService, parameters);
        validateResult(result);
    }

    private void validateResult(final String json) throws Exception {
        final List<OperatorDrillRoamingAnalysisQueryResult> results = getTranslator().translateResult(json,
                OperatorDrillRoamingAnalysisQueryResult.class);
        assertThat(results.size(), is(2));

        final OperatorDrillRoamingAnalysisQueryResult operatorWithMostRoamingErrors = results.get(0);
        assertThat(operatorWithMostRoamingErrors.getOperatorName(), is(MOVISTAR));
        assertThat(operatorWithMostRoamingErrors.getNumErrors(), is(3));
        assertThat(operatorWithMostRoamingErrors.getImpactedSubscribers(), is(2));
        assertThat(operatorWithMostRoamingErrors.getCategoryIDDesc(), is("Call Setup Failures"));
        assertThat(operatorWithMostRoamingErrors.getMcc(), is("722"));
        assertThat(operatorWithMostRoamingErrors.getMnc(), is("010"));

        final OperatorDrillRoamingAnalysisQueryResult operatorWithSecondMostRoamingErrors = results.get(1);
        assertThat(operatorWithSecondMostRoamingErrors.getOperatorName(), is(MOVISTAR));
        assertThat(operatorWithSecondMostRoamingErrors.getNumErrors(), is(2));
        assertThat(operatorWithSecondMostRoamingErrors.getImpactedSubscribers(), is(2));
        assertThat(operatorWithSecondMostRoamingErrors.getCategoryIDDesc(), is("Call Drops"));
        assertThat(operatorWithMostRoamingErrors.getMcc(), is("722"));
        assertThat(operatorWithMostRoamingErrors.getMnc(), is("010"));
    }
}
