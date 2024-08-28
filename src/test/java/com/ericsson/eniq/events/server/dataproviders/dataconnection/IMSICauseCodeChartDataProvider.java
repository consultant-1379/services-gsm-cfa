/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.dataproviders.dataconnection;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.resources.gsm.GSMApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.automation.util.CombinationUtils.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import com.ericsson.eniq.events.server.test.automation.util.CombinationGenerator;
import com.ericsson.eniq.events.server.test.automation.util.CombinationGeneratorImpl;

/**
 * @author ejoegaf
 * @since 2012
 *
 */
public class IMSICauseCodeChartDataProvider {
    public static Object[] provideTestData() {

        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>()
                .add(TIME_QUERY_PARAM, FIVE_MINUTES, THIRTY_MINUTES, ONE_DAY, ONE_WEEK)
                .add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET)
                .add(MAX_ROWS, DEFAULT_MAX_ROWS)
                .add(IMSI_PARAM, TEST_VALUE_IMSI + "")
                .add(TYPE_PARAM, TYPE_IMSI)
                .add(CAUSE_CODE_ID_LIST, "0,2", "0,2," + DATA_CONNECTION_SUCCESS_CAUSE_CODE_ID_LABEL,
                        DATA_CONNECTION_SUCCESS_CAUSE_CODE_ID_LABEL + ",0,2",
                        DATA_CONNECTION_SUCCESS_CAUSE_CODE_ID_LABEL).build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());
    }
}
