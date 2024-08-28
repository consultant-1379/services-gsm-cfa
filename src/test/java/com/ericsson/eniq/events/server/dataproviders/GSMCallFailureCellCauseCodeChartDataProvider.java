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
 * @author ewanggu
 * @since 2011
 *
 */
public class GSMCallFailureCellCauseCodeChartDataProvider {
    public static Object[] provideTestData() {

        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>()
                .add(DISPLAY_PARAM, CHART_PARAM).add(TIME_QUERY_PARAM, FIVE_MINUTES, THIRTY_MINUTES, ONE_DAY, ONE_WEEK)
                .add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET).add(MAX_ROWS, DEFAULT_MAX_ROWS)
                .add(NODE_PARAM, TEST_VALUE_MSS_CELL_NODE).add(TYPE_PARAM, TYPE_CELL).add(CAUSE_CODE_ID_LIST, "1,2")
                .build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());
    }
}