package com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.roaming;

import static com.ericsson.eniq.events.server.common.ApplicationConfigConstants.*;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.TechPackData.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ejb.Local;
import javax.ejb.Stateless;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.lang.math.NumberUtils;

import com.ericsson.eniq.events.server.common.TechPackList;
import com.ericsson.eniq.events.server.common.tablesandviews.AggregationTableInfo;
import com.ericsson.eniq.events.server.query.QueryParameter;
import com.ericsson.eniq.events.server.resources.gsm.GSMApplicationConstants;
import com.ericsson.eniq.events.server.serviceprovider.Service;
import com.ericsson.eniq.events.server.serviceprovider.impl.GenericService;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author elasabu
 * 2012
 */
@Stateless
@Local(Service.class)
public class CountryRoamingDetailService extends GenericService {

    @Override
    public String getTemplatePath() {
        return GSM_CFA_COUNTRY_ROAMING_DETAIL_ANALYSIS;
    }

    @Override
    public Map<String, Object> getServiceSpecificTemplateParameters(
            final MultivaluedMap<String, String> requestParameters, final FormattedDateTimeRange dateTimeRange,
            final TechPackList techPackList) {
        final Map<String, Object> serviceSpecificTemplateParameters = new HashMap<String, Object>();
        if (requestParameters.containsKey(CATEGORY_ID) && NumberUtils.isNumber(requestParameters.getFirst(CATEGORY_ID))) {
            serviceSpecificTemplateParameters.put(CATEGORY_ID, requestParameters.getFirst(CATEGORY_ID));
        } else {
            serviceSpecificTemplateParameters.put(CATEGORY_ID, "");
        }
        if (requestParameters.containsKey(COUNTRY)) {
            serviceSpecificTemplateParameters.put(COUNTRY, requestParameters.getFirst(COUNTRY));
        }
        return serviceSpecificTemplateParameters;
    }

    @Override
    public Map<String, Object> getServiceSpecificDataServiceParameters(
            final MultivaluedMap<String, String> requestParameters) {
        final Map<String, Object> dataServiceParameters = new HashMap<String, Object>();
        dataServiceParameters.put(TZ_OFFSET, requestParameters.getFirst(TZ_OFFSET));
        return dataServiceParameters;
    }

    @Override
    public Map<String, QueryParameter> getServiceSpecificQueryParameters(
            final MultivaluedMap<String, String> requestParameters) {
        final Map<String, QueryParameter> queryParameters = new HashMap<String, QueryParameter>();
        if (requestParameters.containsKey(CATEGORY_ID) && NumberUtils.isNumber(requestParameters.getFirst(CATEGORY_ID))) {
            queryParameters.put(CATEGORY_ID,
                    QueryParameter.createIntParameter(Integer.parseInt(requestParameters.getFirst(CATEGORY_ID))));

        }
        if (requestParameters.containsKey(MCC_PARAM)) {
            queryParameters.put(IMSI_MCC_PARAM,
                    QueryParameter.createStringParameter(requestParameters.getFirst(MCC_PARAM)));
        }
        queryParameters.put(GSMApplicationConstants.ROAMING_PARAM,
                QueryParameter.createIntParameter(Integer.parseInt("1")));

        return queryParameters;
    }

    @Override
    public List<String> getRequiredParametersForQuery() {
        return new ArrayList<String>();
    }

    @Override
    public MultivaluedMap<String, String> getStaticParameters() {
        return new MultivaluedMapImpl();
    }

    @Override
    public String getDrillDownTypeForService(final MultivaluedMap<String, String> requestParameters) {
        return null;
    }

    @Override
    public AggregationTableInfo getAggregationView(final String type) {
        return new AggregationTableInfo(NO_AGGREGATION_AVAILABLE);
    }

    @Override
    public List<String> getApplicableTechPacks(final MultivaluedMap<String, String> requestParameters) {
        final List<String> techpacks = new ArrayList<String>();
        techpacks.add(EVENT_E_GSM_CFA);
        return techpacks;
    }

    @Override
    public boolean areRawTablesRequiredForAggregationQueries() {
        return true;
    }

    @Override
    public int getMaxAllowableSize() {
        return MAXIMUM_POSSIBLE_CONFIGURABLE_MAX_JSON_RESULT_SIZE;
    }

    @Override
    public boolean requiredToCheckValidParameterValue(final MultivaluedMap<String, String> requestParameters) {
        return false;
    }

    @Override
    public String getTableSuffixKey() {
        return null;
    }

    @Override
    public List<String> getMeasurementTypes() {
        return null;
    }

    @Override
    public List<String> getRawTableKeys() {
        final List<String> rawTableKeys = new ArrayList<String>();
        rawTableKeys.add(KEY_TYPE_ERR);
        return rawTableKeys;
    }

}
