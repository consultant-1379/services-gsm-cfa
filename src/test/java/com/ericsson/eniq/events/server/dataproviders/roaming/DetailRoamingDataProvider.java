package com.ericsson.eniq.events.server.dataproviders.roaming;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.automation.util.CombinationUtils.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import com.ericsson.eniq.events.server.test.automation.util.CombinationGenerator;
import com.ericsson.eniq.events.server.test.automation.util.CombinationGeneratorImpl;

public class DetailRoamingDataProvider {

    public static Object[] provideTestData() {
        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>()
                .add(TIME_QUERY_PARAM, THIRTY_MINUTES, FIVE_MINUTES, ONE_DAY, ONE_WEEK)
                .add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET).add(MAX_ROWS, DEFAULT_MAX_ROWS, NO_MAX_ROWS)
                .add(MCC, TEST_VALUE_MCC).add(COUNTRY, TEST_VALUE_COUNTRY)
                .add(CATEGORY_ID, GSM_CALL_SETUP_FAILURE_CATEGORY_ID).build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());
    }
}
