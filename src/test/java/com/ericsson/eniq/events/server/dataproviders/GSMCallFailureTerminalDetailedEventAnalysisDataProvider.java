/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.dataproviders;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.CATEGORY_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.DISPLAY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.GRID_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.GSM_CALL_DROP_CATEGORY_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.MAX_ROWS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TAC;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TIME_QUERY_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.test.automation.util.CombinationUtils.convertToArrayOfMultivaluedMap;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.CAUSE_GROUP;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.DEFAULT_MAX_ROWS;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.EXTENDED_CAUSE;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.FIVE_MINUTES;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.ONE_DAY;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.ONE_WEEK;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TEST_VALUE_TAC;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.TEST_VALUE_TIMEZONE_OFFSET;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.THIRTY_MINUTES;

import com.ericsson.eniq.events.server.test.automation.util.CombinationGenerator;
import com.ericsson.eniq.events.server.test.automation.util.CombinationGeneratorImpl;

/**
 * @author ejoegaf
 * @since 2011
 * 
 */
public class GSMCallFailureTerminalDetailedEventAnalysisDataProvider {

	private static final String CAUSE_CODE_1 = "1";
	private static final String EXTENDE_CAUSE_1 = "60";

	public static Object[] provideTestData() {

		final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>()
				.add(DISPLAY_PARAM, GRID_PARAM)
				.add(TIME_QUERY_PARAM, FIVE_MINUTES, THIRTY_MINUTES, ONE_DAY,
						ONE_WEEK).add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET)
				.add(MAX_ROWS, DEFAULT_MAX_ROWS).add(TAC, TEST_VALUE_TAC)
				.add(CAUSE_GROUP, CAUSE_CODE_1)
				.add(EXTENDED_CAUSE, EXTENDE_CAUSE_1)
				.add(CATEGORY_ID, GSM_CALL_DROP_CATEGORY_ID).build();
		return convertToArrayOfMultivaluedMap(combinationGenerator
				.getAllCombinations());
	}
}