package com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.roaming;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.TechPackData.*;
import static com.ericsson.eniq.events.server.common.tablesandviews.TableKeys.*;
import static com.ericsson.eniq.events.server.resources.gsm.GSMApplicationConstants.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ejb.Local;
import javax.ejb.Stateless;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.common.EventDataSourceType;
import com.ericsson.eniq.events.server.common.TechPackList;
import com.ericsson.eniq.events.server.common.tablesandviews.AggregationTableInfo;
import com.ericsson.eniq.events.server.query.QueryParameter;
import com.ericsson.eniq.events.server.serviceprovider.Service;
import com.ericsson.eniq.events.server.serviceprovider.impl.GenericService;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ewzacdv
 * 2012
 */
@Stateless
@Local(Service.class)
public class CountryCallFailureSummaryService extends GenericService {

    @Override
    public String getTemplatePath() {
        return GSM_CALL_FAILURE_COUNTRY_ROAMING_ANALYSIS_SUMMARY;
    }

    @Override
    public Map<String, Object> getServiceSpecificTemplateParameters(
            final MultivaluedMap<String, String> requestParameters, final FormattedDateTimeRange dateTimeRange,
            final TechPackList techPackList) {
        final Map<String, Object> serviceSpecificTemplateParameters = new HashMap<String, Object>();
        serviceSpecificTemplateParameters.put(ERR_AGGREGATION_VIEW, techPackList.getTechPack(EVENT_E_GSM_CFA)
                .getPlainAggregationView());
        serviceSpecificTemplateParameters.put(SUC_AGGREGATION_VIEW, techPackList.getTechPack(EVENT_E_GSM_CFA)
                .getSucAggregationView());
        if (requestParameters.containsKey(TYPE_ROAMING_COUNTRY)) {
            serviceSpecificTemplateParameters.put(TYPE_ROAMING_COUNTRY,
                    requestParameters.getFirst(TYPE_ROAMING_COUNTRY));
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
        return new HashMap<String, QueryParameter>();
    }

    @Override
    public List<String> getRequiredParametersForQuery() {
        final List<String> requiredParameters = new ArrayList<String>();
        requiredParameters.add(TZ_OFFSET);
        return requiredParameters;
    }

    @Override
    public MultivaluedMap<String, String> getStaticParameters() {
        return new MultivaluedMapImpl();
    }

    @Override
    public String getDrillDownTypeForService(final MultivaluedMap<String, String> requestParameters) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AggregationTableInfo getAggregationView(final String type) {
        return new AggregationTableInfo(MCC_MNC_ROAM, EventDataSourceType.AGGREGATED_DAY,
                EventDataSourceType.AGGREGATED_15MIN);
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
        return MAXIMUM_NUMBER_OF_OPERATORS;
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
        rawTableKeys.add(ERR);
        rawTableKeys.add(SUC);
        return rawTableKeys;
    }
}
