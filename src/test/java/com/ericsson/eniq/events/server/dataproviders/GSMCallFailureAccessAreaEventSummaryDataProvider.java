/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.dataproviders;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.automation.util.CombinationUtils.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import com.ericsson.eniq.events.server.test.automation.util.CombinationGenerator;
import com.ericsson.eniq.events.server.test.automation.util.CombinationGeneratorImpl;

/**
 * @author ehorpte
 * @since 2011
 *
 */
public class GSMCallFailureAccessAreaEventSummaryDataProvider {

    public static Object[] provideTestData() {
        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>()
                .add(NODE_PARAM, "GSMCELL1,,BSC1,Ericsson,GSM").add(TYPE_PARAM, TYPE_CELL)
                .add(TIME_QUERY_PARAM, THIRTY_MINUTES, FIVE_MINUTES, ONE_DAY, ONE_WEEK)
                .add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET).add(MAX_ROWS, DEFAULT_MAX_ROWS, NO_MAX_ROWS).build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());
    }

}
