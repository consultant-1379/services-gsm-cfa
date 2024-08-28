/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.roaming.OperatorRoamingSummaryService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.OperatorRoamingSummaryQueryResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author eatiaro
 * 2012
 *
 */
public class OperatorRoamingSummaryAggTest extends
        BaseDataIntegrityTest<OperatorRoamingSummaryQueryResult> {

    private OperatorRoamingSummaryService service;

    private final int noOfErrorsForMovistar = 3;

    private final int noOfErrorsForRoshan = 2;

    @Before
    public void setup() throws Exception {
        service = new OperatorRoamingSummaryService();
        attachDependencies(service);
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_SGEH_MCCMNC);
        createAndPopulateLookupTable(TEMP_DIM_E_SGEH_MCCMNC);
        createAggTable();
        createRawTable();

        insertData();
    }

    private void insertData() throws SQLException {
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        insertRowIntoAggTable(MCC_FOR_ARGENTINA, MNC_FOR_MOVISTAR, noOfErrorsForMovistar, timestamp);
        insertRowIntoRawTable(MCC_FOR_ARGENTINA, MNC_FOR_MOVISTAR, SAMPLE_IMSI, timestamp);
        insertRowIntoRawTable(MCC_FOR_ARGENTINA, MNC_FOR_MOVISTAR, SAMPLE_IMSI, timestamp);
        insertRowIntoRawTable(MCC_FOR_ARGENTINA, MNC_FOR_MOVISTAR, SAMPLE_IMSI, timestamp);

        insertRowIntoAggTable(MCC_FOR_AFGHANISTAN, MNC_FOR_ROSHAN, noOfErrorsForRoshan, timestamp);
        insertRowIntoRawTable(MCC_FOR_AFGHANISTAN, MNC_FOR_ROSHAN, SAMPLE_IMSI, timestamp);
        insertRowIntoRawTable(MCC_FOR_AFGHANISTAN, MNC_FOR_ROSHAN, SAMPLE_IMSI_2, timestamp);

    }

    private void insertRowIntoRawTable(final String mcc, final String mnc, final long imsi, final String timestamp)
            throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(IMSI_MCC, mcc);
        values.put(IMSI_MNC, mnc);
        values.put(IMSI, imsi);
        values.put(ROAMING, 1);
        values.put(TAC, SAMPLE_TAC);
        values.put(DATETIME_ID, timestamp);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, values);

    }

    private void insertRowIntoAggTable(final String mcc, final String mnc, final int noOfErrors, final String timestamp)
            throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(IMSI_MCC, mcc);
        values.put(IMSI_MNC, mnc);
        values.put(NO_OF_ERRORS, noOfErrors);
        values.put(DATETIME_ID, timestamp);
        insertRow(TEMP_EVENT_E_GSM_CFA_MCC_MNC_ROAM_DAY, values);

    }

    private void createRawTable() throws Exception {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(IMSI);
        columns.add(IMSI_MCC);
        columns.add(IMSI_MNC);
        columns.add(ROAMING);
        columns.add(TAC);
        columns.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columns);

    }

    private void createAggTable() throws Exception {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(IMSI_MCC);
        columns.add(IMSI_MNC);
        columns.add(NO_OF_ERRORS);
        columns.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_MCC_MNC_ROAM_DAY, columns);

    }

    @Test
    public void testOneWeekQuery() throws Exception {
        final MultivaluedMap<String, String> parameters = new MultivaluedMapImpl();
        parameters.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        parameters.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        final String result = runQuery(service, parameters);
        validateResult(result);
    }

    private void validateResult(final String json) throws Exception {
        validateAgainstGridDefinition(json, "ROAMING_ANALYSIS_RAN_GSM_OPERATOR");
        final List<OperatorRoamingSummaryQueryResult> results = getTranslator().translateResult(json,
                OperatorRoamingSummaryQueryResult.class);
        assertThat(results.size(), is(2));
        final OperatorRoamingSummaryQueryResult operatorWithMostRoamingErrors = results.get(0);
        assertThat(operatorWithMostRoamingErrors.getOperator(), is(MOVISTAR));
        assertThat(operatorWithMostRoamingErrors.getNumErrors(), is(noOfErrorsForMovistar));
        assertThat(operatorWithMostRoamingErrors.getNumRoamers(), is(1));
        assertThat(operatorWithMostRoamingErrors.getCountry(), is(ARGENTINA));
        assertThat(operatorWithMostRoamingErrors.getMCC(), is(MCC_FOR_ARGENTINA));
        assertThat(operatorWithMostRoamingErrors.getMNC(), is(MNC_FOR_MOVISTAR));

        final OperatorRoamingSummaryQueryResult operatorWithSecondMostRoamingErrors = results.get(1);
        assertThat(operatorWithSecondMostRoamingErrors.getOperator(), is(ROSHAN));
        assertThat(operatorWithSecondMostRoamingErrors.getNumErrors(), is(noOfErrorsForRoshan));
        assertThat(operatorWithSecondMostRoamingErrors.getNumRoamers(), is(2));
        assertThat(operatorWithSecondMostRoamingErrors.getCountry(), is(AFGHANISTAN));
        assertThat(operatorWithSecondMostRoamingErrors.getMCC(), is(MCC_FOR_AFGHANISTAN));
        assertThat(operatorWithSecondMostRoamingErrors.getMNC(), is(MNC_FOR_ROSHAN));

    }

}
