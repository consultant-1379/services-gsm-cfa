/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.callfailure.ranking;

import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.ranking.SubscriberRankingService;
import com.ericsson.eniq.events.server.test.queryresults.ResultTranslator;
import com.ericsson.eniq.events.server.test.queryresults.SubscriberCallSetupFailureRankingResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.MultivaluedMap;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.NO_OF_ERRORS;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.ONE_WEEK;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_GSM_CFA_IMSI_RANK_DAY;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author ewanggu
 * @since 2011
 *
 */
public class GSMCallFailureSubscriberCallDropRankingServiceAggTest extends
        BaseDataIntegrityTest<SubscriberCallSetupFailureRankingResult> {

    private SubscriberRankingService subscriberCallDropRankingService;

    private static final int TEST_IMSI_0 = 0;

    private static final int TEST_IMSI_1 = 11111119;

    private static final int TEST_IMSI_2 = 22222229;

    private static final int TEST_IMSI_3 = 33333339;

    @Before
    public void onSetUp() throws Exception {
        subscriberCallDropRankingService = new SubscriberRankingService();
        attachDependencies(subscriberCallDropRankingService);
        createTables();
        insertData();
    }

    /*
     * The expected outcome is for IMSI_1 to have 3 Failures while IMSI_2 will have only 2.
     * They should be ranked accordingly.
     */
    @Test
    public void testGetRankingData_SUBSCRIBER_CFA() throws Exception {

        final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
        requestParameters.putSingle(DISPLAY_PARAM, "grid");
        requestParameters.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        requestParameters.putSingle(TZ_OFFSET, "+0100");
        requestParameters.putSingle(MAX_ROWS, "10");

        final String json = runQuery(subscriberCallDropRankingService, requestParameters);

        System.out.println(json);

        final ResultTranslator<SubscriberCallSetupFailureRankingResult> rt = getTranslator();
        final List<SubscriberCallSetupFailureRankingResult> rankingResult = rt.translateResult(json,
                SubscriberCallSetupFailureRankingResult.class);

        assertThat("There should be exactly 3 results!", rankingResult.size(), is(3));

        final SubscriberCallSetupFailureRankingResult worstSubscriber = rankingResult.get(0);
        assertThat(worstSubscriber.getRank(), is(1));
        assertThat(worstSubscriber.getImsi(), is(TEST_IMSI_1));
        assertThat("worstSubscriber should have exactly 3 CallDrop events!", worstSubscriber.getNumFailures(), is(3));

        final SubscriberCallSetupFailureRankingResult nextWorstSubscriber = rankingResult.get(1);
        assertThat(nextWorstSubscriber.getRank(), is(2));
        assertThat(nextWorstSubscriber.getImsi(), is(TEST_IMSI_2));
        assertThat("nextWorstSubscriber should have exactly 2 CallDrop events!", nextWorstSubscriber.getNumFailures(),
                is(2));

        final SubscriberCallSetupFailureRankingResult lastWorstSubscriber = rankingResult.get(2);
        assertThat(lastWorstSubscriber.getRank(), is(3));
        assertThat(lastWorstSubscriber.getImsi(), is(TEST_IMSI_3));
        assertThat("nextWorstSubscriber should have exactly 1 CallDrop events!", lastWorstSubscriber.getNumFailures(),
                is(1));

    }

    private void createTables() throws Exception {

        final Collection<String> columnsForEETable = new ArrayList<String>();
        columnsForEETable.add(IMSI);
        columnsForEETable.add(CATEGORY_ID);
        columnsForEETable.add(NO_OF_ERRORS);
        columnsForEETable.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_GSM_CFA_IMSI_RANK_DAY, columnsForEETable);
    }

    /*
     *      4 CallDrops for IMSI_0
     *      3 CallDrops for IMSI_1
     *      2 CallDrops for IMSI_2
     *      1 CallDrops for IMSI_3
     */
    private void insertData() throws Exception {

        final String dateTime = DateTimeUtilities.getDateTimeMinus36Hours();

        insertIMSI(TEST_IMSI_0, dateTime, GSM_CALL_DROP_CATEGORY_ID, 4);

        insertIMSI(TEST_IMSI_1, dateTime, GSM_CALL_DROP_CATEGORY_ID, 3);

        insertIMSI(TEST_IMSI_2, dateTime, GSM_CALL_DROP_CATEGORY_ID, 2);

        insertIMSI(TEST_IMSI_3, dateTime, GSM_CALL_DROP_CATEGORY_ID, 1);

    }

    private void insertIMSI(final int imsi, final String dt, final String eventID, final int failNum)
            throws SQLException {

        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(IMSI, imsi);
        valuesForTable.put(CATEGORY_ID, Integer.valueOf(eventID));
        valuesForTable.put(NO_OF_ERRORS, failNum);
        valuesForTable.put(DATETIME_ID, dt);
        insertRow(TEMP_EVENT_E_GSM_CFA_IMSI_RANK_DAY, valuesForTable);
    }
}
