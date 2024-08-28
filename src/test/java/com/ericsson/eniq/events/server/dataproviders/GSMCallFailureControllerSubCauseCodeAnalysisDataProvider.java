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
 * @author ejoegaf
 * @since 2011
 *
 */
public class GSMCallFailureControllerSubCauseCodeAnalysisDataProvider {

    private static final String EXCESSIVE_TA = "EXCESSIVE TA";

    private static final String CAUSE_CODE_1 = "1";

    public static Object[] provideTestData() {
        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>()
                .add(DISPLAY_PARAM, GRID_PARAM).add(TIME_QUERY_PARAM, FIVE_MINUTES, THIRTY_MINUTES, ONE_DAY, ONE_WEEK)
                .add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET).add(MAX_ROWS, DEFAULT_MAX_ROWS)
                .add(NODE_PARAM, TEST_BSC1_NODE).add(TYPE_PARAM, BSC).add(CAUSE_CODE_PARAM, CAUSE_CODE_1)
                .add(CAUSE_CODE_DESCRIPTION, EXCESSIVE_TA).build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());
    }
}