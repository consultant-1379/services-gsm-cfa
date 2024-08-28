/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.dataconnection.events;

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
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.dataconnection.events.SubscriberCCDetailedService;
import com.ericsson.eniq.events.server.test.queryresults.gsm.dataconnection.CCDetailedEAImsiResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author eramiye
 * @since 2012
 *
 */
public class CCDetailedEAImsiRawTest extends BaseDataIntegrityTest<CCDetailedEAImsiResult> {

    private SubscriberCCDetailedService service;

    @Before
    public void setup() throws Exception {
        service = new SubscriberCCDetailedService();
        attachDependencies(service);
        createEventTable();
        createLookupTables();
        insertLookupData();
        insertDataIntoTacGroupTable();
    }

    @Test
    public void testFiveMinuteQuery() throws URISyntaxException, Exception {
        insertEventData(DateTimeUtilities.getDateTimeMinusMinutes(2));
        insertEventData(DateTimeUtilities.getDateTimeMinusMinutes(10));//should not be included in results
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TIME_QUERY_PARAM, FIVE_MINUTES);
        requestParameters.add(TYPE_PARAM, TYPE_IMSI);
        requestParameters.add(IMSI_PARAM, TEST_VALUE_IMSI1);
        requestParameters.add(CAUSE_CODE_PARAM, TEST_VALUE_TBF_RELEASE_CAUSE);
        final String result = runQuery(service, requestParameters);
        verifyResult(result, TEST_VALUE_GSM_CELL1_NAME, TEST_VALUE_GSM_CONTROLLER1_NAME, TEST_VALUE_TAC,
                TEST_VALUE_MANUFACTURER, TEST_VALUE_MARKETING_NAME, 9/*we expect 9 because we also want the entry with exclusive TAC counted*/);
    }

    @Test
    public void testTwoHourQuery() throws URISyntaxException, Exception {
        insertEventData(DateTimeUtilities.getDateTimeMinusMinutes(120));
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TIME_QUERY_PARAM, "120");
        requestParameters.add(TYPE_PARAM, TYPE_IMSI);
        requestParameters.add(IMSI_PARAM, TEST_VALUE_IMSI1);
        requestParameters.add(CAUSE_CODE_PARAM, TEST_VALUE_TBF_RELEASE_CAUSE);
        final String result = runQuery(service, requestParameters);
        verifyResult(result, TEST_VALUE_GSM_CELL1_NAME, TEST_VALUE_GSM_CONTROLLER1_NAME, TEST_VALUE_TAC,
                TEST_VALUE_MANUFACTURER, TEST_VALUE_MARKETING_NAME, 9/*we expect 9 because we also want the entry with exclusive TAC counted*/);
    }

    @Test
    public void testLeftOuterJoins() throws URISyntaxException, Exception {
        insertEventDataForLeftOuterJoin(DateTimeUtilities.getDateTimeMinusMinutes(2));
        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.add(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        requestParameters.add(TIME_QUERY_PARAM, FIVE_MINUTES);
        requestParameters.add(TYPE_PARAM, TYPE_IMSI);
        requestParameters.add(IMSI_PARAM, TEST_VALUE_IMSI1);
        requestParameters.add(CAUSE_CODE_PARAM, TEST_VALUE_TBF_RELEASE_CAUSE);
        final String result = runQuery(service, requestParameters);

        verifyResult(result, "", "", TEST_VALUE_TAC_NO_LOOKUP, "", "", 1);
        verifyLeftOuterJoinValues(result);
    }

    private void verifyResult(final String json, final String expectedCellName, final String expectedBscName,
            final int expectedTAC, final String expectedManufacturer, final String expectedMarketingName,
            final int expectedResultCount) throws Exception {
        assertJSONSucceeds(json);
        validateAgainstGridDefinition(json, "GSM_DATA_CONN_EVENT_ANALYSIS_FROM_SUBPIE");
        final List<CCDetailedEAImsiResult> results = getTranslator()
                .translateResult(json, CCDetailedEAImsiResult.class);

        assertThat(results.size(), is(expectedResultCount));
        final CCDetailedEAImsiResult result = results.get(0);
        assertThat(result.getEventType(), is(TEST_VALUE_CATEGORY_ID_DESC));
        assertThat(result.getAccessArea(), is(expectedCellName));
        assertThat(result.getController(), is(expectedBscName));
        assertThat(result.getTac(), is(expectedTAC));
        assertThat(result.getTerminalMake(), is(expectedManufacturer));
        assertThat(result.getTerminalModel(), is(expectedMarketingName));
        assertThat(result.getTbfReleaseCause(), is(TEST_VALUE_TBF_RELEASE_CAUSE_DESC));
        assertThat(result.getTbfDataVolume(), is(TEST_VALUE_TBF_DATA_VOLUME));
        assertThat(result.getTbfDuration(), is(TEST_VALUE_TBF_DURATION));
    }

    private void verifyLeftOuterJoinValues(final String json) throws Exception {
        final CCDetailedEAImsiResult jsonResult = getTranslator().translateResult(json, CCDetailedEAImsiResult.class)
                .get(0);
        assertThat(jsonResult.getTerminalMake(), is(""));
        assertThat(jsonResult.getTerminalModel(), is(""));
        assertThat(jsonResult.getGprsMeasReportRxqualDlDesc(), is(""));
        assertThat(jsonResult.getGprsMeasReportCvalueDesc(), is(""));
        assertThat(jsonResult.getGprsMeasReportCvBepDesc(), is(""));
        assertThat(jsonResult.getGprsMeasReportMeanBepDesc(), is(""));
        assertThat(jsonResult.getGprsMeasReportSignVarDesc(), is(""));
        assertThat(jsonResult.getChannelRelatedReleaseCauseGroupDesc(), is(""));
        assertThat(jsonResult.getAccessArea(), is(""));
        assertThat(jsonResult.getController(), is(""));
    }

    private void insertEventData(final String dateTime) throws Exception {
        insertData(TEST_VALUE_IMSI1, TEST_VALUE_TAC, dateTime, TEST_VALUE_HIER321_ID,
                TEST_VALUE_GPRS_MEAS_REPORT_RXQUAL_DL, TEST_VALUE_GPRS_MEAS_REPORT_SIGN_VAR,
                TEST_VALUE_GPRS_MEAS_REPORT_MEAN_BEP, TEST_VALUE_GPRS_MEAS_REPORT_CV_BEP,
                TEST_VALUE_GPRS_MEAS_REPORT_CVALUE, TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP, 3);
        insertData(TEST_VALUE_IMSI1, TEST_VALUE_TAC, dateTime, TEST_VALUE_HIER321_ID2,
                TEST_VALUE_GPRS_MEAS_REPORT_RXQUAL_DL, TEST_VALUE_GPRS_MEAS_REPORT_SIGN_VAR,
                TEST_VALUE_GPRS_MEAS_REPORT_MEAN_BEP, TEST_VALUE_GPRS_MEAS_REPORT_CV_BEP,
                TEST_VALUE_GPRS_MEAS_REPORT_CVALUE, TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP, 2);
        insertData(TEST_VALUE_IMSI1, TEST_VALUE_TAC, dateTime, TEST_VALUE_HIER321_ID,
                TEST_VALUE_GPRS_MEAS_REPORT_RXQUAL_DL, TEST_VALUE_GPRS_MEAS_REPORT_SIGN_VAR,
                TEST_VALUE_GPRS_MEAS_REPORT_MEAN_BEP, TEST_VALUE_GPRS_MEAS_REPORT_CV_BEP,
                TEST_VALUE_GPRS_MEAS_REPORT_CVALUE, TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_CONTROL, 3);
        //exclusive tac data - not excluded for an IMSI query

        //exclusive tac data
        insertData(TEST_VALUE_IMSI1, SAMPLE_EXCLUSIVE_TAC, dateTime, TEST_VALUE_HIER321_ID,
                TEST_VALUE_GPRS_MEAS_REPORT_RXQUAL_DL, TEST_VALUE_GPRS_MEAS_REPORT_SIGN_VAR,
                TEST_VALUE_GPRS_MEAS_REPORT_MEAN_BEP, TEST_VALUE_GPRS_MEAS_REPORT_CV_BEP,
                TEST_VALUE_GPRS_MEAS_REPORT_CVALUE, TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP, 1);
    }

    private void insertEventDataForLeftOuterJoin(final String dateTime) throws SQLException {

        insertData(TEST_VALUE_IMSI1, TEST_VALUE_TAC_NO_LOOKUP, dateTime, TEST_VALUE_HIER321_ID_NO_LOOKUP,
                TEST_VALUE_GPRS_MEAS_REPORT_RXQUAL_DL_NO_LOOKUP, TEST_VALUE_GPRS_MEAS_REPORT_SIGN_VAR_NO_LOOKUP,
                TEST_VALUE_GPRS_MEAS_REPORT_MEAN_BEP_NO_LOOKUP, TEST_VALUE_GPRS_MEAS_REPORT_CV_BEP_NO_LOOKUP,
                TEST_VALUE_GPRS_MEAS_REPORT_CVALUE_NO_LOOKUP, TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_NO_LOOKUP,
                1);
    }

    private void insertData(final String imsi, final int tac, final String time, final String hier321_id,
            final String gprsMeasReportRxqualDlDesc, final String gprsMeasReportSignVarDesc,
            final String gprsMeasReportMeanBepDesc, final String gprsMeasReportCvBepDesc,
            final String gprsMeasReportCvalueDesc, final String channelRelatedReleaseCauseGroupDesc, final int instances)
            throws SQLException {
        for (int i = 0; i < instances; i++) {
            final Map<String, Object> dataForEventTable = new HashMap<String, Object>();
            dataForEventTable.put(HIER3_ID, TEST_VALUE_HIER3_ID);
            dataForEventTable.put(HIER321_ID, hier321_id);
            dataForEventTable.put(TAC, tac);
            dataForEventTable.put(EVENT_TIME, time);
            dataForEventTable.put(TIMEZONE, "0");
            dataForEventTable.put(IMSI, imsi);
            dataForEventTable.put(CATEGORY_ID, TEST_VALUE_CATEGORY_ID);
            dataForEventTable.put(DATETIME_ID, time);
            dataForEventTable.put(TBF_RELEASE_CAUSE, TEST_VALUE_TBF_RELEASE_CAUSE);
            dataForEventTable.put(TBF_DATA_VOLUME, TEST_VALUE_TBF_DATA_VOLUME);
            dataForEventTable.put(TBF_DURATION, TEST_VALUE_TBF_DURATION);
            dataForEventTable.put(RP_NUMBER, TEST_VALUE_RP_NUMBER);
            dataForEventTable.put(CHANNEL_RELATED_RELEASE_CAUSE, TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE);
            dataForEventTable.put(CHANNEL_RELATED_RELEASE_CAUSE_GROUP, channelRelatedReleaseCauseGroupDesc);
            dataForEventTable.put(TBF_MUX, TEST_VALUE_TBF_MUX);
            dataForEventTable.put(EFACTOR_SETTINGS, TEST_VALUE_EFACTOR_SETTINGS);
            dataForEventTable.put(DATA_VALID_INDICATOR, TEST_VALUE_DATA_VALID_INDICATOR);
            dataForEventTable.put(MS_SAIC_CAP, TEST_VALUE_MS_SAIC_CAP);
            dataForEventTable.put(AQM_ACTIVE, TEST_VALUE_AQM_ACTIVE);
            dataForEventTable.put(DTM_FLAG, TEST_VALUE_DTM_FLAG);
            dataForEventTable.put(RLC_MODE, TEST_VALUE_RLC_MODE);
            dataForEventTable.put(DIR, TEST_VALUE_DIR);
            dataForEventTable.put(OFLW, TEST_VALUE_OFLW);
            dataForEventTable.put(MS_3GPP_CAP, TEST_VALUE_MS_3GPP_CAP);
            dataForEventTable.put(TTI_MODE, TEST_VALUE_TTI_MODE);
            dataForEventTable.put(REDUCED_LATENCY, TEST_VALUE_REDUCED_LATENCY);
            dataForEventTable.put(RADIO_LINK_BITRATE, TEST_VALUE_RADIO_LINK_BITRATE);
            dataForEventTable.put(GPRS_MEAS_REPORT_RXQUAL_DL, gprsMeasReportRxqualDlDesc);
            dataForEventTable.put(GPRS_MEAS_REPORT_SIGN_VAR, gprsMeasReportSignVarDesc);
            dataForEventTable.put(GPRS_MEAS_REPORT_MEAN_BEP, gprsMeasReportMeanBepDesc);
            dataForEventTable.put(GPRS_MEAS_REPORT_CV_BEP, gprsMeasReportCvBepDesc);
            dataForEventTable.put(GPRS_MEAS_REPORT_CVALUE, gprsMeasReportCvalueDesc);
            dataForEventTable.put(IP_LATENCY, TEST_VALUE_IP_LATENCY);
            dataForEventTable.put(LOW_PRIORITY_MODE_TIME, TEST_VALUE_LOW_PRIORITY_MODE_TIME);
            dataForEventTable.put(BLER, TEST_VALUE_BLER);
            dataForEventTable.put(MS_FREQ_BAND_CAP_GSM_850, TEST_VALUE_MS_FREQ_BAND_CAP_GSM_850);
            dataForEventTable.put(MS_FREQ_BAND_CAP_GSM_900, TEST_VALUE_MS_FREQ_BAND_CAP_GSM_900);
            dataForEventTable.put(MS_FREQ_BAND_CAP_GSM_1800, TEST_VALUE_MS_FREQ_BAND_CAP_GSM_1800);
            dataForEventTable.put(MS_FREQ_BAND_CAP_GSM_1900, TEST_VALUE_MS_FREQ_BAND_CAP_GSM_1900);
            dataForEventTable.put(AQM_DATA_DELIVERED, TEST_VALUE_AQM_DATA_DELIVERED);
            dataForEventTable.put(AQM_DATA_RECIEVED, TEST_VALUE_AQM_DATA_RECIEVED);
            dataForEventTable.put(PAN_INDICATOR, TEST_VALUE_PAN_INDICATOR);
            dataForEventTable.put(TBF_MODE, TEST_VALUE_TBF_MODE);
            dataForEventTable.put(DCDL_CAPABILITY, TEST_VALUE_DCDL_CAPABILITY);
            dataForEventTable.put(DCDL_INDICATOR, TEST_VALUE_DCDL_INDICATOR);
            dataForEventTable.put(MS_MSLOT_CAP_REDUCTION, TEST_VALUE_MS_MSLOT_CAP_REDUCTION);
            insertRow(TEMP_EVENT_E_GSM_PS_ERR_RAW, dataForEventTable);
        }
    }

    private void createLookupTables() throws Exception {

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_TBF_RELEASE_CAUSE, DIM_E_GSM_PS_TBF_RELEASE_CAUSE,
                TBF_RELEASE_CAUSE, TBF_RELEASE_CAUSE_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_TBF_MUX, DIM_E_GSM_PS_TBF_MUX, TBF_MUX, TBF_MUX_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_DATA_VALID_INDICATOR, DIM_E_GSM_PS_DATA_VALID_INDICATOR,
                DATA_VALID_INDICATOR, DATA_VALID_INDICATOR_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_MS_SAIC_CAP, DIM_E_GSM_PS_MS_SAIC_CAP, MS_SAIC_CAP,
                MS_SAIC_CAP_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_AQM_ACTIVE, DIM_E_GSM_PS_AQM_ACTIVE, AQM_ACTIVE, AQM_ACTIVE_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_DTM_FLAG, DIM_E_GSM_PS_DTM_FLAG, DTM_FLAG, DTM_FLAG_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_RLC_MODE, DIM_E_GSM_PS_RLC_MODE, RLC_MODE, RLC_MODE_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_DIR, DIM_E_GSM_PS_DIR, DIR, DIR_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_OFLW, DIM_E_GSM_PS_OFLW, OFLW, OFLW_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_MS_3GPP_CAP, DIM_E_GSM_PS_MS_3GPP_CAP, MS_3GPP_CAP,
                MS_3GPP_CAP_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_TTI_MODE, DIM_E_GSM_PS_TTI_MODE, TTI_MODE, TTI_MODE_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_REDUCED_LATENCY, DIM_E_GSM_PS_REDUCED_LATENCY, REDUCED_LATENCY,
                REDUCED_LATENCY_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_GPRS_MEAS_REPORT_RXQUAL_DL,
                DIM_E_GSM_PS_GPRS_MEAS_REPORT_RXQUAL_DL, GPRS_MEAS_REPORT_RXQUAL_DL, GPRS_MEAS_REPORT_RXQUAL_DL_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_GPRS_MEAS_REPORT_SIGN_VAR,
                DIM_E_GSM_PS_GPRS_MEAS_REPORT_SIGN_VAR, GPRS_MEAS_REPORT_SIGN_VAR, GPRS_MEAS_REPORT_SIGN_VAR_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_GPRS_MEAS_REPORT_MEAN_BEP,
                DIM_E_GSM_PS_GPRS_MEAS_REPORT_MEAN_BEP, GPRS_MEAS_REPORT_MEAN_BEP, GPRS_MEAS_REPORT_MEAN_BEP_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_GPRS_MEAS_REPORT_CV_BEP, DIM_E_GSM_PS_GPRS_MEAS_REPORT_CV_BEP,
                GPRS_MEAS_REPORT_CV_BEP, GPRS_MEAS_REPORT_CV_BEP_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_GPRS_MEAS_REPORT_CVALUE, DIM_E_GSM_PS_GPRS_MEAS_REPORT_CVALUE,
                GPRS_MEAS_REPORT_CVALUE, GPRS_MEAS_REPORT_CVALUE_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_MS_FREQ_BAND_CAP_GSM_850, DIM_E_GSM_PS_MS_FREQ_BAND_CAP_GSM_850,
                MS_FREQ_BAND_CAP_GSM_850, MS_FREQ_BAND_CAP_GSM_850_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_MS_FREQ_BAND_CAP_GSM_900, DIM_E_GSM_PS_MS_FREQ_BAND_CAP_GSM_900,
                MS_FREQ_BAND_CAP_GSM_900, MS_FREQ_BAND_CAP_GSM_900_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_MS_FREQ_BAND_CAP_GSM_1800,
                DIM_E_GSM_PS_MS_FREQ_BAND_CAP_GSM_1800, MS_FREQ_BAND_CAP_GSM_1800, MS_FREQ_BAND_CAP_GSM_1800_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_MS_FREQ_BAND_CAP_GSM_1900,
                DIM_E_GSM_PS_MS_FREQ_BAND_CAP_GSM_1900, MS_FREQ_BAND_CAP_GSM_1900, MS_FREQ_BAND_CAP_GSM_1900_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_PAN_INDICATOR, DIM_E_GSM_PS_PAN_INDICATOR, PAN_INDICATOR,
                PAN_INDICATOR_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_TBF_MODE, DIM_E_GSM_PS_TBF_MODE, TBF_MODE, TBF_MODE_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_CHANNEL_RELATED_RELEASE_CAUSE_GROUP,
                DIM_E_GSM_PS_CHANNEL_RELATED_RELEASE_CAUSE_GROUP, CHANNEL_RELATED_RELEASE_CAUSE_GROUP,
                CHANNEL_RELATED_RELEASE_CAUSE_GROUP_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_DCDL_CAPABILITY, DIM_E_GSM_PS_DCDL_CAPABILITY, DCDL_CAPABILITY,
                DCDL_CAPABILITY_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_DCDL_INDICATOR, DIM_E_GSM_PS_DCDL_INDICATOR, DCDL_INDICATOR,
                DCDL_INDICATOR_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_MS_MSLOT_CAP_REDUCTION, DIM_E_GSM_PS_MS_MSLOT_CAP_REDUCTION,
                MS_MSLOT_CAP_REDUCTION, MS_MSLOT_CAP_REDUCTION_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_GSM_PS_EVENTTYPE, DIM_E_GSM_PS_EVENTTYPE, CATEGORY_ID, CATEGORY_ID_DESC);

        createAndReplaceLookupTable(TEMP_DIM_E_SGEH_HIER321, DIM_E_SGEH_HIER321, HIERARCHY_3, HIER321_ID, HIERARCHY_1,
                HIER3_ID, VENDOR, RAT);
        createAndReplaceLookupTable(TEMP_DIM_E_SGEH_TAC, DIM_E_SGEH_TAC, MANUFACTURER, MARKETING_NAME, TAC);
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

    private void insertLookupData() throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(MANUFACTURER, TEST_VALUE_MANUFACTURER);
        valuesForTable.put(MARKETING_NAME, TEST_VALUE_MARKETING_NAME);
        valuesForTable.put(TAC, TEST_VALUE_TAC);
        insertRow(TEMP_DIM_E_SGEH_TAC, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(MANUFACTURER, TEST_VALUE_MANUFACTURER_CONTROL);
        valuesForTable.put(MARKETING_NAME, TEST_VALUE_MARKETING_NAME_CONTROL);
        valuesForTable.put(TAC, TEST_VALUE_TAC_CONTROL);
        insertRow(TEMP_DIM_E_SGEH_TAC, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(TBF_RELEASE_CAUSE, TEST_VALUE_TBF_RELEASE_CAUSE);
        valuesForTable.put(TBF_RELEASE_CAUSE_DESC, TEST_VALUE_TBF_RELEASE_CAUSE_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_TBF_RELEASE_CAUSE, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(TBF_RELEASE_CAUSE, TEST_VALUE_TBF_RELEASE_CAUSE_CONTROL);
        valuesForTable.put(TBF_RELEASE_CAUSE_DESC, TEST_VALUE_TBF_RELEASE_CAUSE_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_TBF_RELEASE_CAUSE, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(TBF_MUX, TEST_VALUE_TBF_MUX);
        valuesForTable.put(TBF_MUX_DESC, TEST_VALUE_TBF_MUX_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_TBF_MUX, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(TBF_MUX, TEST_VALUE_TBF_MUX_CONTROL);
        valuesForTable.put(TBF_MUX_DESC, TEST_VALUE_TBF_MUX_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_TBF_MUX, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(DATA_VALID_INDICATOR, TEST_VALUE_DATA_VALID_INDICATOR);
        valuesForTable.put(DATA_VALID_INDICATOR_DESC, TEST_VALUE_DATA_VALID_INDICATOR_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_DATA_VALID_INDICATOR, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(DATA_VALID_INDICATOR, TEST_VALUE_DATA_VALID_INDICATOR_CONTROL);
        valuesForTable.put(DATA_VALID_INDICATOR_DESC, TEST_VALUE_DATA_VALID_INDICATOR_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_DATA_VALID_INDICATOR, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(MS_SAIC_CAP, TEST_VALUE_MS_SAIC_CAP);
        valuesForTable.put(MS_SAIC_CAP_DESC, TEST_VALUE_MS_SAIC_CAP_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_MS_SAIC_CAP, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(MS_SAIC_CAP, TEST_VALUE_MS_SAIC_CAP_CONTROL);
        valuesForTable.put(MS_SAIC_CAP_DESC, TEST_VALUE_MS_SAIC_CAP_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_MS_SAIC_CAP, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(AQM_ACTIVE, TEST_VALUE_AQM_ACTIVE);
        valuesForTable.put(AQM_ACTIVE_DESC, TEST_VALUE_AQM_ACTIVE_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_AQM_ACTIVE, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(AQM_ACTIVE, TEST_VALUE_AQM_ACTIVE_CONTROL);
        valuesForTable.put(AQM_ACTIVE_DESC, TEST_VALUE_AQM_ACTIVE_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_AQM_ACTIVE, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(DTM_FLAG, TEST_VALUE_DTM_FLAG);
        valuesForTable.put(DTM_FLAG_DESC, TEST_VALUE_DTM_FLAG_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_DTM_FLAG, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(DTM_FLAG, TEST_VALUE_DTM_FLAG_CONTROL);
        valuesForTable.put(DTM_FLAG_DESC, TEST_VALUE_DTM_FLAG_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_DTM_FLAG, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(RLC_MODE, TEST_VALUE_RLC_MODE);
        valuesForTable.put(RLC_MODE_DESC, TEST_VALUE_RLC_MODE_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_RLC_MODE, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(RLC_MODE, TEST_VALUE_RLC_MODE_CONTROL);
        valuesForTable.put(RLC_MODE_DESC, TEST_VALUE_RLC_MODE_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_RLC_MODE, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(DIR, TEST_VALUE_DIR);
        valuesForTable.put(DIR_DESC, TEST_VALUE_DIR_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_DIR, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(DIR, TEST_VALUE_DIR_CONTROL);
        valuesForTable.put(DIR_DESC, TEST_VALUE_DIR_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_DIR, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(OFLW, TEST_VALUE_OFLW);
        valuesForTable.put(OFLW_DESC, TEST_VALUE_OFLW_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_OFLW, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(OFLW, TEST_VALUE_OFLW_CONTROL);
        valuesForTable.put(OFLW_DESC, TEST_VALUE_OFLW_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_OFLW, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(MS_3GPP_CAP, TEST_VALUE_MS_3GPP_CAP);
        valuesForTable.put(MS_3GPP_CAP_DESC, TEST_VALUE_MS_3GPP_CAP_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_MS_3GPP_CAP, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(MS_3GPP_CAP, TEST_VALUE_MS_3GPP_CAP_CONTROL);
        valuesForTable.put(MS_3GPP_CAP_DESC, TEST_VALUE_MS_3GPP_CAP_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_MS_3GPP_CAP, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(TTI_MODE, TEST_VALUE_TTI_MODE);
        valuesForTable.put(TTI_MODE_DESC, TEST_VALUE_TTI_MODE_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_TTI_MODE, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(TTI_MODE, TEST_VALUE_TTI_MODE_CONTROL);
        valuesForTable.put(TTI_MODE_DESC, TEST_VALUE_TTI_MODE_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_TTI_MODE, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(REDUCED_LATENCY, TEST_VALUE_REDUCED_LATENCY);
        valuesForTable.put(REDUCED_LATENCY_DESC, TEST_VALUE_REDUCED_LATENCY_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_REDUCED_LATENCY, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(REDUCED_LATENCY, TEST_VALUE_REDUCED_LATENCY_CONTROL);
        valuesForTable.put(REDUCED_LATENCY_DESC, TEST_VALUE_REDUCED_LATENCY_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_REDUCED_LATENCY, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(GPRS_MEAS_REPORT_RXQUAL_DL, TEST_VALUE_GPRS_MEAS_REPORT_RXQUAL_DL);
        valuesForTable.put(GPRS_MEAS_REPORT_RXQUAL_DL_DESC, TEST_VALUE_GPRS_MEAS_REPORT_RXQUAL_DL_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_GPRS_MEAS_REPORT_RXQUAL_DL, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(GPRS_MEAS_REPORT_RXQUAL_DL, TEST_VALUE_GPRS_MEAS_REPORT_RXQUAL_DL_CONTROL);
        valuesForTable.put(GPRS_MEAS_REPORT_RXQUAL_DL_DESC, TEST_VALUE_GPRS_MEAS_REPORT_RXQUAL_DL_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_GPRS_MEAS_REPORT_RXQUAL_DL, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(GPRS_MEAS_REPORT_SIGN_VAR, TEST_VALUE_GPRS_MEAS_REPORT_SIGN_VAR);
        valuesForTable.put(GPRS_MEAS_REPORT_SIGN_VAR_DESC, TEST_VALUE_GPRS_MEAS_REPORT_SIGN_VAR_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_GPRS_MEAS_REPORT_SIGN_VAR, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(GPRS_MEAS_REPORT_SIGN_VAR, TEST_VALUE_GPRS_MEAS_REPORT_SIGN_VAR_CONTROL);
        valuesForTable.put(GPRS_MEAS_REPORT_SIGN_VAR_DESC, TEST_VALUE_GPRS_MEAS_REPORT_SIGN_VAR_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_GPRS_MEAS_REPORT_SIGN_VAR, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(GPRS_MEAS_REPORT_MEAN_BEP, TEST_VALUE_GPRS_MEAS_REPORT_MEAN_BEP);
        valuesForTable.put(GPRS_MEAS_REPORT_MEAN_BEP_DESC, TEST_VALUE_GPRS_MEAS_REPORT_MEAN_BEP_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_GPRS_MEAS_REPORT_MEAN_BEP, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(GPRS_MEAS_REPORT_MEAN_BEP, TEST_VALUE_GPRS_MEAS_REPORT_MEAN_BEP_CONTROL);
        valuesForTable.put(GPRS_MEAS_REPORT_MEAN_BEP_DESC, TEST_VALUE_GPRS_MEAS_REPORT_MEAN_BEP_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_GPRS_MEAS_REPORT_MEAN_BEP, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(GPRS_MEAS_REPORT_CV_BEP, TEST_VALUE_GPRS_MEAS_REPORT_CV_BEP);
        valuesForTable.put(GPRS_MEAS_REPORT_CV_BEP_DESC, TEST_VALUE_GPRS_MEAS_REPORT_CV_BEP_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_GPRS_MEAS_REPORT_CV_BEP, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(GPRS_MEAS_REPORT_CV_BEP, TEST_VALUE_GPRS_MEAS_REPORT_CV_BEP_CONTROL);
        valuesForTable.put(GPRS_MEAS_REPORT_CV_BEP_DESC, TEST_VALUE_GPRS_MEAS_REPORT_CV_BEP_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_GPRS_MEAS_REPORT_CV_BEP, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(GPRS_MEAS_REPORT_CVALUE, TEST_VALUE_GPRS_MEAS_REPORT_CVALUE);
        valuesForTable.put(GPRS_MEAS_REPORT_CVALUE_DESC, TEST_VALUE_GPRS_MEAS_REPORT_CVALUE_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_GPRS_MEAS_REPORT_CVALUE, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(GPRS_MEAS_REPORT_CVALUE, TEST_VALUE_GPRS_MEAS_REPORT_CVALUE_CONTROL);
        valuesForTable.put(GPRS_MEAS_REPORT_CVALUE_DESC, TEST_VALUE_GPRS_MEAS_REPORT_CVALUE_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_GPRS_MEAS_REPORT_CVALUE, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(MS_FREQ_BAND_CAP_GSM_850, TEST_VALUE_MS_FREQ_BAND_CAP_GSM_850);
        valuesForTable.put(MS_FREQ_BAND_CAP_GSM_850_DESC, TEST_VALUE_MS_FREQ_BAND_CAP_GSM_850_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_MS_FREQ_BAND_CAP_GSM_850, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(MS_FREQ_BAND_CAP_GSM_850, TEST_VALUE_MS_FREQ_BAND_CAP_GSM_850_CONTROL);
        valuesForTable.put(MS_FREQ_BAND_CAP_GSM_850_DESC, TEST_VALUE_MS_FREQ_BAND_CAP_GSM_850_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_MS_FREQ_BAND_CAP_GSM_850, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(MS_FREQ_BAND_CAP_GSM_900, TEST_VALUE_MS_FREQ_BAND_CAP_GSM_900);
        valuesForTable.put(MS_FREQ_BAND_CAP_GSM_900_DESC, TEST_VALUE_MS_FREQ_BAND_CAP_GSM_900_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_MS_FREQ_BAND_CAP_GSM_900, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(MS_FREQ_BAND_CAP_GSM_900, TEST_VALUE_MS_FREQ_BAND_CAP_GSM_900_CONTROL);
        valuesForTable.put(MS_FREQ_BAND_CAP_GSM_900_DESC, TEST_VALUE_MS_FREQ_BAND_CAP_GSM_900_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_MS_FREQ_BAND_CAP_GSM_900, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(MS_FREQ_BAND_CAP_GSM_1800, TEST_VALUE_MS_FREQ_BAND_CAP_GSM_1800);
        valuesForTable.put(MS_FREQ_BAND_CAP_GSM_1800_DESC, TEST_VALUE_MS_FREQ_BAND_CAP_GSM_1800_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_MS_FREQ_BAND_CAP_GSM_1800, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(MS_FREQ_BAND_CAP_GSM_1800, TEST_VALUE_MS_FREQ_BAND_CAP_GSM_1800_CONTROL);
        valuesForTable.put(MS_FREQ_BAND_CAP_GSM_1800_DESC, TEST_VALUE_MS_FREQ_BAND_CAP_GSM_1800_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_MS_FREQ_BAND_CAP_GSM_1800, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(MS_FREQ_BAND_CAP_GSM_1900, TEST_VALUE_MS_FREQ_BAND_CAP_GSM_1900);
        valuesForTable.put(MS_FREQ_BAND_CAP_GSM_1900_DESC, TEST_VALUE_MS_FREQ_BAND_CAP_GSM_1900_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_MS_FREQ_BAND_CAP_GSM_1900, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(MS_FREQ_BAND_CAP_GSM_1900, TEST_VALUE_MS_FREQ_BAND_CAP_GSM_1900_CONTROL);
        valuesForTable.put(MS_FREQ_BAND_CAP_GSM_1900_DESC, TEST_VALUE_MS_FREQ_BAND_CAP_GSM_1900_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_MS_FREQ_BAND_CAP_GSM_1900, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(PAN_INDICATOR, TEST_VALUE_PAN_INDICATOR);
        valuesForTable.put(PAN_INDICATOR_DESC, TEST_VALUE_PAN_INDICATOR_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_PAN_INDICATOR, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(PAN_INDICATOR, TEST_VALUE_PAN_INDICATOR_CONTROL);
        valuesForTable.put(PAN_INDICATOR_DESC, TEST_VALUE_PAN_INDICATOR_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_PAN_INDICATOR, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(TBF_MODE, TEST_VALUE_TBF_MODE);
        valuesForTable.put(TBF_MODE_DESC, TEST_VALUE_TBF_MODE_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_TBF_MODE, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(TBF_MODE, TEST_VALUE_TBF_MODE_CONTROL);
        valuesForTable.put(TBF_MODE_DESC, TEST_VALUE_TBF_MODE_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_TBF_MODE, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(DCDL_CAPABILITY, TEST_VALUE_DCDL_CAPABILITY);
        valuesForTable.put(DCDL_CAPABILITY_DESC, TEST_VALUE_DCDL_CAPABILITY_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_DCDL_CAPABILITY, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(DCDL_CAPABILITY, TEST_VALUE_DCDL_CAPABILITY_CONTROL);
        valuesForTable.put(DCDL_CAPABILITY_DESC, TEST_VALUE_DCDL_CAPABILITY_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_DCDL_CAPABILITY, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(DCDL_INDICATOR, TEST_VALUE_DCDL_INDICATOR);
        valuesForTable.put(DCDL_INDICATOR_DESC, TEST_VALUE_DCDL_INDICATOR_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_DCDL_INDICATOR, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(DCDL_INDICATOR, TEST_VALUE_DCDL_INDICATOR_CONTROL);
        valuesForTable.put(DCDL_INDICATOR_DESC, TEST_VALUE_DCDL_INDICATOR_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_DCDL_INDICATOR, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(MS_MSLOT_CAP_REDUCTION, TEST_VALUE_MS_MSLOT_CAP_REDUCTION);
        valuesForTable.put(MS_MSLOT_CAP_REDUCTION_DESC, TEST_VALUE_MS_MSLOT_CAP_REDUCTION_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_MS_MSLOT_CAP_REDUCTION, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(MS_MSLOT_CAP_REDUCTION, TEST_VALUE_MS_MSLOT_CAP_REDUCTION_CONTROL);
        valuesForTable.put(MS_MSLOT_CAP_REDUCTION_DESC, TEST_VALUE_MS_MSLOT_CAP_REDUCTION_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_MS_MSLOT_CAP_REDUCTION, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(CHANNEL_RELATED_RELEASE_CAUSE_GROUP, TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP);
        valuesForTable.put(CHANNEL_RELATED_RELEASE_CAUSE_GROUP_DESC,
                TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_CHANNEL_RELATED_RELEASE_CAUSE_GROUP, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(CHANNEL_RELATED_RELEASE_CAUSE_GROUP, TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_CONTROL);
        valuesForTable.put(CHANNEL_RELATED_RELEASE_CAUSE_GROUP_DESC,
                TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_CHANNEL_RELATED_RELEASE_CAUSE_GROUP, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(CATEGORY_ID, TEST_VALUE_CATEGORY_ID);
        valuesForTable.put(CATEGORY_ID_DESC, TEST_VALUE_CATEGORY_ID_DESC);
        insertRow(TEMP_DIM_E_GSM_PS_EVENTTYPE, valuesForTable);
        valuesForTable.clear();
        valuesForTable.put(CATEGORY_ID, TEST_VALUE_CATEGORY_ID_CONTROL);
        valuesForTable.put(CATEGORY_ID_DESC, TEST_VALUE_CATEGORY_ID_DESC_CONTROL);
        insertRow(TEMP_DIM_E_GSM_PS_EVENTTYPE, valuesForTable);

        valuesForTable.clear();
        valuesForTable.put(HIERARCHY_3, TEST_VALUE_GSM_CONTROLLER1_NAME);
        valuesForTable.put(HIER321_ID, TEST_VALUE_HIER321_ID);
        valuesForTable.put(HIERARCHY_1, TEST_VALUE_GSM_CELL1_NAME);
        valuesForTable.put(HIER3_ID, TEST_VALUE_HIER3_ID);
        valuesForTable.put(VENDOR, ERICSSON);
        valuesForTable.put(RAT, "0");
        insertRow(TEMP_DIM_E_SGEH_HIER321, valuesForTable);
    }

    private void insertDataIntoTacGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SAMPLE_EXCLUSIVE_TAC);
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    private void createEventTable() throws Exception {
        final Collection<String> columnsForEventTable = new ArrayList<String>();
        columnsForEventTable.add(HIER3_ID);
        columnsForEventTable.add(HIER321_ID);
        columnsForEventTable.add(TAC);
        columnsForEventTable.add(EVENT_TIME);
        columnsForEventTable.add(TIMEZONE);
        columnsForEventTable.add(IMSI);
        columnsForEventTable.add(CATEGORY_ID);
        columnsForEventTable.add(DATETIME_ID);
        columnsForEventTable.add(TBF_RELEASE_CAUSE);
        columnsForEventTable.add(TBF_DATA_VOLUME);
        columnsForEventTable.add(TBF_DURATION);
        columnsForEventTable.add(RP_NUMBER);
        columnsForEventTable.add(CHANNEL_RELATED_RELEASE_CAUSE);
        columnsForEventTable.add(CHANNEL_RELATED_RELEASE_CAUSE_GROUP);
        columnsForEventTable.add(TBF_MUX);
        columnsForEventTable.add(EFACTOR_SETTINGS);
        columnsForEventTable.add(DATA_VALID_INDICATOR);
        columnsForEventTable.add(MS_SAIC_CAP);
        columnsForEventTable.add(AQM_ACTIVE);
        columnsForEventTable.add(DTM_FLAG);
        columnsForEventTable.add(RLC_MODE);
        columnsForEventTable.add(DIR);
        columnsForEventTable.add(OFLW);
        columnsForEventTable.add(MS_3GPP_CAP);
        columnsForEventTable.add(TTI_MODE);
        columnsForEventTable.add(REDUCED_LATENCY);
        columnsForEventTable.add(RADIO_LINK_BITRATE);
        columnsForEventTable.add(GPRS_MEAS_REPORT_RXQUAL_DL);
        columnsForEventTable.add(GPRS_MEAS_REPORT_SIGN_VAR);
        columnsForEventTable.add(GPRS_MEAS_REPORT_MEAN_BEP);
        columnsForEventTable.add(GPRS_MEAS_REPORT_CV_BEP);
        columnsForEventTable.add(GPRS_MEAS_REPORT_CVALUE);
        columnsForEventTable.add(IP_LATENCY);
        columnsForEventTable.add(LOW_PRIORITY_MODE_TIME);
        columnsForEventTable.add(BLER);
        columnsForEventTable.add(MS_FREQ_BAND_CAP_GSM_850);
        columnsForEventTable.add(MS_FREQ_BAND_CAP_GSM_900);
        columnsForEventTable.add(MS_FREQ_BAND_CAP_GSM_1800);
        columnsForEventTable.add(MS_FREQ_BAND_CAP_GSM_1900);
        columnsForEventTable.add(AQM_DATA_DELIVERED);
        columnsForEventTable.add(AQM_DATA_RECIEVED);
        columnsForEventTable.add(PAN_INDICATOR);
        columnsForEventTable.add(TBF_MODE);
        columnsForEventTable.add(DCDL_CAPABILITY);
        columnsForEventTable.add(DCDL_INDICATOR);
        columnsForEventTable.add(MS_MSLOT_CAP_REDUCTION);
        createTemporaryTable(TEMP_EVENT_E_GSM_PS_ERR_RAW, columnsForEventTable);
    }

    private static String TEST_VALUE_CATEGORY_ID = "2";

    private static final String TEST_VALUE_HIER321_ID = "4948639634796658772";

    private static final String TEST_VALUE_HIER321_ID_NO_LOOKUP = "4948639634796652345";

    private static final String TEST_VALUE_HIER3_ID = "5386564559998864911";

    private static final String TEST_VALUE_CATEGORY_ID_DESC = "Call Drops";

    private static final String TEST_VALUE_IMSI1 = "404685505601234";

    private static final int TEST_VALUE_TAC = 100100;

    private static final int TEST_VALUE_TAC_NO_LOOKUP = 300300;

    private static final String TEST_VALUE_MANUFACTURER = "Mitsubishi";

    private static final String TEST_VALUE_MARKETING_NAME = "G410";

    private static final String TEST_VALUE_MANUFACTURER_CONTROL = "htc";

    private static final String TEST_VALUE_MARKETING_NAME_CONTROL = "senstaion";

    private static final int TEST_VALUE_TAC_CONTROL = 200200;

    private static final String TEST_VALUE_TBF_RELEASE_CAUSE = "0";

    private static final String TEST_VALUE_TBF_RELEASE_CAUSE_DESC = "flush";

    private static final String TEST_VALUE_TBF_RELEASE_CAUSE_CONTROL = "1";

    private static final String TEST_VALUE_TBF_RELEASE_CAUSE_DESC_CONTROL = "ms fault";

    private static final String TEST_VALUE_TBF_MUX = "0";

    private static final String TEST_VALUE_TBF_MUX_DESC = "muxed";

    private static final String TEST_VALUE_TBF_MUX_CONTROL = "1";

    private static final String TEST_VALUE_TBF_MUX_DESC_CONTROL = "not muxed";

    private static final String TEST_VALUE_DATA_VALID_INDICATOR = "0";

    private static final String TEST_VALUE_DATA_VALID_INDICATOR_DESC = "value";

    private static final String TEST_VALUE_DATA_VALID_INDICATOR_CONTROL = "1";

    private static final String TEST_VALUE_DATA_VALID_INDICATOR_DESC_CONTROL = "control";

    private static final String TEST_VALUE_MS_SAIC_CAP = "0";

    private static final String TEST_VALUE_MS_SAIC_CAP_DESC = "value";

    private static final String TEST_VALUE_MS_SAIC_CAP_CONTROL = "1";

    private static final String TEST_VALUE_MS_SAIC_CAP_DESC_CONTROL = "control";

    private static final String TEST_VALUE_AQM_ACTIVE = "0";

    private static final String TEST_VALUE_AQM_ACTIVE_DESC = "value";

    private static final String TEST_VALUE_AQM_ACTIVE_CONTROL = "1";

    private static final String TEST_VALUE_AQM_ACTIVE_DESC_CONTROL = "control";

    private static final String TEST_VALUE_DTM_FLAG = "0";

    private static final String TEST_VALUE_DTM_FLAG_DESC = "value";

    private static final String TEST_VALUE_DTM_FLAG_CONTROL = "1";

    private static final String TEST_VALUE_DTM_FLAG_DESC_CONTROL = "control";

    private static final String TEST_VALUE_RLC_MODE = "0";

    private static final String TEST_VALUE_RLC_MODE_DESC = "value";

    private static final String TEST_VALUE_RLC_MODE_CONTROL = "1";

    private static final String TEST_VALUE_RLC_MODE_DESC_CONTROL = "control";

    private static final String TEST_VALUE_DIR = "0";

    private static final String TEST_VALUE_DIR_DESC = "value";

    private static final String TEST_VALUE_DIR_CONTROL = "1";

    private static final String TEST_VALUE_DIR_DESC_CONTROL = "control";

    private static final String TEST_VALUE_OFLW = "0";

    private static final String TEST_VALUE_OFLW_DESC = "value";

    private static final String TEST_VALUE_OFLW_CONTROL = "1";

    private static final String TEST_VALUE_OFLW_DESC_CONTROL = "control";

    private static final String TEST_VALUE_MS_3GPP_CAP = "0";

    private static final String TEST_VALUE_MS_3GPP_CAP_DESC = "value";

    private static final String TEST_VALUE_MS_3GPP_CAP_CONTROL = "1";

    private static final String TEST_VALUE_MS_3GPP_CAP_DESC_CONTROL = "control";

    private static final String TEST_VALUE_TTI_MODE = "0";

    private static final String TEST_VALUE_TTI_MODE_DESC = "value";

    private static final String TEST_VALUE_TTI_MODE_CONTROL = "1";

    private static final String TEST_VALUE_TTI_MODE_DESC_CONTROL = "control";

    private static final String TEST_VALUE_REDUCED_LATENCY = "0";

    private static final String TEST_VALUE_REDUCED_LATENCY_DESC = "value";

    private static final String TEST_VALUE_REDUCED_LATENCY_CONTROL = "1";

    private static final String TEST_VALUE_REDUCED_LATENCY_DESC_CONTROL = "control";

    private static final String TEST_VALUE_GPRS_MEAS_REPORT_RXQUAL_DL = "0";

    private static final String TEST_VALUE_GPRS_MEAS_REPORT_RXQUAL_DL_DESC = "value";

    private static final String TEST_VALUE_GPRS_MEAS_REPORT_RXQUAL_DL_CONTROL = "1";

    private static final String TEST_VALUE_GPRS_MEAS_REPORT_RXQUAL_DL_DESC_CONTROL = "control";

    private static final String TEST_VALUE_GPRS_MEAS_REPORT_RXQUAL_DL_NO_LOOKUP = "2";

    private static final String TEST_VALUE_GPRS_MEAS_REPORT_SIGN_VAR = "0";

    private static final String TEST_VALUE_GPRS_MEAS_REPORT_SIGN_VAR_DESC = "value";

    private static final String TEST_VALUE_GPRS_MEAS_REPORT_SIGN_VAR_CONTROL = "1";

    private static final String TEST_VALUE_GPRS_MEAS_REPORT_SIGN_VAR_DESC_CONTROL = "control";

    private static final String TEST_VALUE_GPRS_MEAS_REPORT_SIGN_VAR_NO_LOOKUP = "2";

    private static final String TEST_VALUE_GPRS_MEAS_REPORT_MEAN_BEP = "0";

    private static final String TEST_VALUE_GPRS_MEAS_REPORT_MEAN_BEP_DESC = "value";

    private static final String TEST_VALUE_GPRS_MEAS_REPORT_MEAN_BEP_CONTROL = "1";

    private static final String TEST_VALUE_GPRS_MEAS_REPORT_MEAN_BEP_DESC_CONTROL = "control";

    private static final String TEST_VALUE_GPRS_MEAS_REPORT_MEAN_BEP_NO_LOOKUP = "2";

    private static final String TEST_VALUE_GPRS_MEAS_REPORT_CV_BEP = "0";

    private static final String TEST_VALUE_GPRS_MEAS_REPORT_CV_BEP_DESC = "value";

    private static final String TEST_VALUE_GPRS_MEAS_REPORT_CV_BEP_CONTROL = "1";

    private static final String TEST_VALUE_GPRS_MEAS_REPORT_CV_BEP_DESC_CONTROL = "control";

    private static final String TEST_VALUE_GPRS_MEAS_REPORT_CV_BEP_NO_LOOKUP = "2";

    private static final String TEST_VALUE_GPRS_MEAS_REPORT_CVALUE = "0";

    private static final String TEST_VALUE_GPRS_MEAS_REPORT_CVALUE_DESC = "value";

    private static final String TEST_VALUE_GPRS_MEAS_REPORT_CVALUE_CONTROL = "1";

    private static final String TEST_VALUE_GPRS_MEAS_REPORT_CVALUE_DESC_CONTROL = "control";

    private static final String TEST_VALUE_GPRS_MEAS_REPORT_CVALUE_NO_LOOKUP = "2";

    private static final String TEST_VALUE_MS_FREQ_BAND_CAP_GSM_850 = "0";

    private static final String TEST_VALUE_MS_FREQ_BAND_CAP_GSM_850_DESC = "value";

    private static final String TEST_VALUE_MS_FREQ_BAND_CAP_GSM_850_CONTROL = "1";

    private static final String TEST_VALUE_MS_FREQ_BAND_CAP_GSM_850_DESC_CONTROL = "control";

    private static final String TEST_VALUE_MS_FREQ_BAND_CAP_GSM_900 = "0";

    private static final String TEST_VALUE_MS_FREQ_BAND_CAP_GSM_900_DESC = "value";

    private static final String TEST_VALUE_MS_FREQ_BAND_CAP_GSM_900_CONTROL = "1";

    private static final String TEST_VALUE_MS_FREQ_BAND_CAP_GSM_900_DESC_CONTROL = "control";

    private static final String TEST_VALUE_MS_FREQ_BAND_CAP_GSM_1800 = "0";

    private static final String TEST_VALUE_MS_FREQ_BAND_CAP_GSM_1800_DESC = "value";

    private static final String TEST_VALUE_MS_FREQ_BAND_CAP_GSM_1800_CONTROL = "1";

    private static final String TEST_VALUE_MS_FREQ_BAND_CAP_GSM_1800_DESC_CONTROL = "control";

    private static final String TEST_VALUE_MS_FREQ_BAND_CAP_GSM_1900 = "0";

    private static final String TEST_VALUE_MS_FREQ_BAND_CAP_GSM_1900_DESC = "value";

    private static final String TEST_VALUE_MS_FREQ_BAND_CAP_GSM_1900_CONTROL = "1";

    private static final String TEST_VALUE_MS_FREQ_BAND_CAP_GSM_1900_DESC_CONTROL = "control";

    private static final String TEST_VALUE_PAN_INDICATOR = "0";

    private static final String TEST_VALUE_PAN_INDICATOR_DESC = "value";

    private static final String TEST_VALUE_PAN_INDICATOR_CONTROL = "1";

    private static final String TEST_VALUE_PAN_INDICATOR_DESC_CONTROL = "control";

    private static final String TEST_VALUE_TBF_MODE = "0";

    private static final String TEST_VALUE_TBF_MODE_DESC = "value";

    private static final String TEST_VALUE_TBF_MODE_CONTROL = "1";

    private static final String TEST_VALUE_TBF_MODE_DESC_CONTROL = "control";

    private static final String TEST_VALUE_DCDL_CAPABILITY = "0";

    private static final String TEST_VALUE_DCDL_CAPABILITY_DESC = "value";

    private static final String TEST_VALUE_DCDL_CAPABILITY_CONTROL = "1";

    private static final String TEST_VALUE_DCDL_CAPABILITY_DESC_CONTROL = "control";

    private static final String TEST_VALUE_DCDL_INDICATOR = "0";

    private static final String TEST_VALUE_DCDL_INDICATOR_DESC = "value";

    private static final String TEST_VALUE_DCDL_INDICATOR_CONTROL = "1";

    private static final String TEST_VALUE_DCDL_INDICATOR_DESC_CONTROL = "control";

    private static final String TEST_VALUE_MS_MSLOT_CAP_REDUCTION = "0";

    private static final String TEST_VALUE_MS_MSLOT_CAP_REDUCTION_DESC = "value";

    private static final String TEST_VALUE_MS_MSLOT_CAP_REDUCTION_CONTROL = "1";

    private static final String TEST_VALUE_MS_MSLOT_CAP_REDUCTION_DESC_CONTROL = "control";

    private static final String TEST_VALUE_CATEGORY_ID_CONTROL = "0";

    private static final String TEST_VALUE_CATEGORY_ID_DESC_CONTROL = "Dummy value";

    private static final String TEST_VALUE_TBF_DATA_VOLUME = "0";

    private static final String TEST_VALUE_TBF_DURATION = "0";

    private static final String TEST_VALUE_RP_NUMBER = "0";

    private static final String TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE = "0";

    private static final String TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP = "0";

    private static final String TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_DESC = "value";

    private static final String TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_CONTROL = "1";

    private static final String TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_DESC_CONTROL = "control";

    private static final String TEST_VALUE_CHANNEL_RELATED_RELEASE_CAUSE_GROUP_NO_LOOKUP = "2";

    private static final String TEST_VALUE_EFACTOR_SETTINGS = "0";

    private static final String TEST_VALUE_RADIO_LINK_BITRATE = "0";

    private static final String TEST_VALUE_IP_LATENCY = "0";

    private static final String TEST_VALUE_LOW_PRIORITY_MODE_TIME = "0";

    private static final String TEST_VALUE_BLER = "0";

    private static final String TEST_VALUE_AQM_DATA_DELIVERED = "0";

    private static final String TEST_VALUE_AQM_DATA_RECIEVED = "0";

    private static final String TEST_VALUE_HIER321_ID2 = "4948639634796658773";

}
