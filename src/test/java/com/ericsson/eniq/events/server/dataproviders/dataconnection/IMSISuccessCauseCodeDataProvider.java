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
public class IMSISuccessCauseCodeDataProvider {
    public static Object[] provideTestData() {

        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>()
                .add(TIME_QUERY_PARAM, FIVE_MINUTES, ONE_WEEK).add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET)
                .add(MAX_ROWS, DEFAULT_MAX_ROWS).add(IMSI_PARAM, TEST_VALUE_IMSI + "")
                .add(CAUSE_CODE, DATA_CONNECTION_SUCCESS_CAUSE_CODE_ID_LABEL).add(TYPE_PARAM, TYPE_IMSI).build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());
    }
}
