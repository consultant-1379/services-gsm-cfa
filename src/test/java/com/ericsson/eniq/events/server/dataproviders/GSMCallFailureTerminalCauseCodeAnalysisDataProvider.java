package com.ericsson.eniq.events.server.dataproviders;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.resources.gsm.GSMApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.automation.util.CombinationUtils.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import com.ericsson.eniq.events.server.test.automation.util.CombinationGenerator;
import com.ericsson.eniq.events.server.test.automation.util.CombinationGeneratorImpl;

public class GSMCallFailureTerminalCauseCodeAnalysisDataProvider {
    public static Object[] provideTestData() {

        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>()
                .add(TAC_LOWER_CASE, "1044000").add(DISPLAY_PARAM, GRID_PARAM)
                .add(TIME_QUERY_PARAM, FIVE_MINUTES, THIRTY_MINUTES, ONE_DAY, ONE_WEEK)
                .add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET).add(MAX_ROWS, DEFAULT_MAX_ROWS).add(TYPE_PARAM, TAC)
                .add(CATEGORY_ID, "0").build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());
    }
}
