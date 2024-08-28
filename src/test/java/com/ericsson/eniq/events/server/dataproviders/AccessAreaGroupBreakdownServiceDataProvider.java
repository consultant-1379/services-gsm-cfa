package com.ericsson.eniq.events.server.dataproviders;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.automation.util.CombinationUtils.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import com.ericsson.eniq.events.server.test.automation.util.CombinationGenerator;
import com.ericsson.eniq.events.server.test.automation.util.CombinationGeneratorImpl;

/**
 * @author ekumjay
 * @since 2012
 *
 */
public class AccessAreaGroupBreakdownServiceDataProvider {
    public static Object[] provideTestData() {

        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>()
                .add(DISPLAY_PARAM, GRID_PARAM).add(TIME_QUERY_PARAM, FIVE_MINUTES, THIRTY_MINUTES, ONE_DAY, ONE_WEEK)
                .add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET).add(MAX_ROWS, DEFAULT_MAX_ROWS).add(TYPE_PARAM, CELL)
                .add(GROUP_NAME_PARAM, TEST_VALUE_CELL_GROUP).add(SUB_CAUSE_CODE_PARAM, TEST_VALUE_SUB_CAUSE_CODE)
                .add(SUB_CAUSE_CODE_DESCRIPTION, TEST_VALUE_SUB_CAUSE_CODE_DESC)
                .add(CAUSE_CODE_PARAM, TEST_VALUE_CAUSE_CODE).add(CAUSE_CODE_DESCRIPTION, TEST_VALUE_CAUSE_CODE_DESC)
                .build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());

    }

}
