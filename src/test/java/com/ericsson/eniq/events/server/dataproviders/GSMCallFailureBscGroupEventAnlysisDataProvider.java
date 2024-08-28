/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.dataproviders;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.automation.util.CombinationUtils.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import com.ericsson.eniq.events.server.test.automation.util.CombinationGenerator;
import com.ericsson.eniq.events.server.test.automation.util.CombinationGeneratorImpl;

/**
 * @author ETHOMIT
 * @since 2012
 *
 */
public class GSMCallFailureBscGroupEventAnlysisDataProvider {
	public static Object[] provideTestData() {

    final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>()
            .add(DISPLAY_PARAM, GRID_PARAM).add(TIME_QUERY_PARAM, FIVE_MINUTES, THIRTY_MINUTES, ONE_DAY, ONE_WEEK)
            .add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET).add(MAX_ROWS, DEFAULT_MAX_ROWS)
            .add(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID).add(TYPE_PARAM, BSC)
            .add(GROUP_NAME_PARAM, TEST_VALUE_BSC_GROUP).build();
    return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());

	}

}
