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

import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.roaming.CountryRoamingSummaryService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.callfailure.CountryRoamingSummaryQueryResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author eatiaro
 * 2012
 *
 */
public class CountryRoamingSummaryAggTest extends BaseDataIntegrityTest<CountryRoamingSummaryQueryResult> {

    private CountryRoamingSummaryService roamingAnalysisService;

    private final int noErrorsForAfghanistan = 3;

    private final int noErrorsForArgentina = 2;

    @Before
    public void setup() throws Exception {
        roamingAnalysisService = new CountryRoamingSummaryService();
        attachDependencies(roamingAnalysisService);
        createAggTable();
        createRawTable();
        insertData();
    }

    private void insertData() throws SQLException {
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        insertRowIntoAggTable(MCC_FOR_AFGHANISTAN, noErrorsForAfghanistan, timestamp);
        insertRowIntoRawTable(MCC_FOR_AFGHANISTAN, SAMPLE_IMSI, timestamp);
        insertRowIntoRawTable(MCC_FOR_AFGHANISTAN, SAMPLE_IMSI, timestamp);
        insertRowIntoRawTable(MCC_FOR_AFGHANISTAN, SAMPLE_IMSI, timestamp);

        insertRowIntoAggTable(MCC_FOR_ARGENTINA, noErrorsForArgentina, timestamp);
        insertRowIntoRawTable(MCC_FOR_ARGENTINA, SAMPLE_IMSI, timestamp);
        insertRowIntoRawTable(MCC_FOR_ARGENTINA, SAMPLE_IMSI_2, timestamp);

    }

    private void insertRowIntoRawTable(final String mcc, final long imsi, final String timestamp) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(IMSI_MCC, mcc);
        values.put(IMSI, imsi);
        values.put(ROAMING, 1);
        values.put(TAC, SAMPLE_TAC);
        values.put(DATETIME_ID, timestamp);
        insertRow(TEMP_EVENT_E_GSM_CFA_ERR_RAW, values);

    }

    private void insertRowIntoAggTable(final String mcc, final int noErrors, final String timestamp)
            throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(IMSI_MCC, mcc);
        values.put(NO_OF_ERRORS, noErrors);
        values.put(DATETIME_ID, timestamp);
        insertRow(TEMP_EVENT_E_GSM_CFA_MCC_MNC_ROAM_DAY, values);

    }

    private void createAggTable() throws Exception {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(IMSI_MCC);
        columns.add(NO_OF_ERRORS);
        columns.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_MCC_MNC_ROAM_DAY, columns);

    }

    private void createRawTable() throws Exception {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(IMSI_MCC);
        columns.add(IMSI);
        columns.add(ROAMING);
        columns.add(TAC);
        columns.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_ERR_RAW, columns);

    }

    @Test
    public void testOneWeekQuery() throws Exception {
        final MultivaluedMap<String, String> parameters = new MultivaluedMapImpl();
        parameters.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        parameters.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        final String result = runQuery(roamingAnalysisService, parameters);
        validateResult(result);
    }

    private void validateResult(final String json) throws Exception {
        validateAgainstGridDefinition(json, "ROAMING_ANALYSIS_RAN_GSM_COUNTRY");
        final List<CountryRoamingSummaryQueryResult> results = getTranslator().translateResult(json,
                CountryRoamingSummaryQueryResult.class);
        assertThat(results.size(), is(2));

        final CountryRoamingSummaryQueryResult countryWithMostRoamingFailures = results.get(0);
        assertThat(countryWithMostRoamingFailures.getCountry(), is(AFGHANISTAN));
        assertThat(countryWithMostRoamingFailures.getNumErrors(), is(noErrorsForAfghanistan));
        assertThat(countryWithMostRoamingFailures.getNumRoamers(), is(1));
        assertThat(countryWithMostRoamingFailures.getMCC(), is(MCC_FOR_AFGHANISTAN));

        final CountryRoamingSummaryQueryResult countryWithSecondMostRoamingFailures = results.get(1);
        assertThat(countryWithSecondMostRoamingFailures.getCountry(), is(ARGENTINE_REPUBLIC));
        assertThat(countryWithSecondMostRoamingFailures.getNumErrors(), is(noErrorsForArgentina));
        assertThat(countryWithSecondMostRoamingFailures.getNumRoamers(), is(2));
        assertThat(countryWithSecondMostRoamingFailures.getMCC(), is(MCC_FOR_ARGENTINA));

    }

}
