package com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.cc;

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

import com.ericsson.eniq.events.server.common.EventDataSourceType;
import com.ericsson.eniq.events.server.common.TechPackList;
import com.ericsson.eniq.events.server.common.tablesandviews.AggregationTableInfo;
import com.ericsson.eniq.events.server.query.QueryParameter;
import com.ericsson.eniq.events.server.serviceprovider.Service;
import com.ericsson.eniq.events.server.serviceprovider.impl.GenericService;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author elasabu
 * @since 2012
 *
 */
@Stateless
@Local(Service.class)
public class ControllerGroupBreakdownService extends GenericService {

    @Override
    public String getTemplatePath() {
        return GSM_CALL_FAILURE_CONTROLLER_GROUP_SUMMARY_BREAKDOWN;
    }

    @Override
    public Map<String, Object> getServiceSpecificTemplateParameters(
            final MultivaluedMap<String, String> requestParameters, final FormattedDateTimeRange dateTimeRange,
            final TechPackList techPackList) {
        final Map<String, Object> serviceSpecificTemplateParameters = new HashMap<String, Object>();

        serviceSpecificTemplateParameters.put(CAUSE_CODE_DESCRIPTION,
                requestParameters.getFirst(CAUSE_CODE_DESCRIPTION));
        serviceSpecificTemplateParameters.put(SUB_CAUSE_CODE_DESCRIPTION,
                requestParameters.getFirst(SUB_CAUSE_CODE_DESCRIPTION));
        serviceSpecificTemplateParameters.put(ERR_AGGREGATION_VIEW, techPackList.getTechPack(EVENT_E_GSM_CFA)
                .getErrAggregationView());
        serviceSpecificTemplateParameters.put(SUC_AGGREGATION_VIEW, techPackList.getTechPack(EVENT_E_GSM_CFA)
                .getSucAggregationView());

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
        requiredParameters.add(GROUP_NAME_PARAM);

        return requiredParameters;
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
        return new AggregationTableInfo("HIER3_EVENTID", EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY);
    }

    @Override
    public List<String> getApplicableTechPacks(final MultivaluedMap<String, String> requestParameters) {
        final List<String> techPacks = new ArrayList<String>();
        techPacks.add(EVENT_E_GSM_CFA);

        return techPacks;
    }

    @Override
    public boolean areRawTablesRequiredForAggregationQueries() {
        return true;
    }

    @Override
    public int getMaxAllowableSize() {
        return DEFAULT_MAXIMUM_JSON_RESULT_SIZE;
    }

    @Override
    public boolean requiredToCheckValidParameterValue(final MultivaluedMap<String, String> requestParameters) {
        if (requestParameters.containsKey(GROUP_NAME_PARAM)) {
            return true;
        }

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
