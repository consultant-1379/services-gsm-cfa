package com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.cc;

import static com.ericsson.eniq.events.server.common.ApplicationConfigConstants.*;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.TechPackData.*;
import static com.ericsson.eniq.events.server.resources.gsm.GSMApplicationConstants.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ejb.Local;
import javax.ejb.Stateless;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.common.TechPackList;
import com.ericsson.eniq.events.server.common.tablesandviews.AggregationTableInfo;
import com.ericsson.eniq.events.server.query.QueryParameter;
import com.ericsson.eniq.events.server.serviceprovider.Service;
import com.ericsson.eniq.events.server.serviceprovider.impl.GenericService;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.sun.jersey.core.util.MultivaluedMapImpl;

@Stateless
@Local(Service.class)
public class TerminalCCService extends GenericService {

    @Override
    public String getTemplatePath() {
        return GSM_CALL_FAILURE_TERMINAL_CAUSE_CODE_ANALYSIS;
    }

    @Override
    public Map<String, Object> getServiceSpecificTemplateParameters(
            final MultivaluedMap<String, String> requestParameters, final FormattedDateTimeRange dateTimeRange,
            final TechPackList techPackList) {
        final Map<String, Object> serviceSpecificTemplateParameters = new HashMap<String, Object>();
        serviceSpecificTemplateParameters.put(TYPE_MAN, requestParameters.getFirst(TYPE_MAN));
        serviceSpecificTemplateParameters.put(MODEL, requestParameters.getFirst(MODEL));
        serviceSpecificTemplateParameters.put(FAILURE_TYPE_PARAM, requestParameters.getFirst(FAILURE_TYPE_PARAM));
        serviceSpecificTemplateParameters.put(ERR_AGGREGATION_VIEW, techPackList.getTechPack(EVENT_E_GSM_CFA)
                .getErrAggregationView());
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
        if (requestParameters.containsKey(CATEGORY_ID)) {
            queryParameters.put(CATEGORY_ID,
                    QueryParameter.createIntParameter(Integer.parseInt(requestParameters.getFirst(CATEGORY_ID))));
        }

        return queryParameters;
    }

    @Override
    public List<String> getRequiredParametersForQuery() {
        final List<String> requiredParameters = new ArrayList<String>();
        requiredParameters.add(TZ_OFFSET);
        requiredParameters.add(TYPE_PARAM);
        return requiredParameters;
    }

    @Override
    public MultivaluedMap<String, String> getStaticParameters() {
        final MultivaluedMap<String, String> staticParameters = new MultivaluedMapImpl();
        staticParameters.add(TYPE_PARAM, TYPE_TAC);
        return staticParameters;
    }

    @Override
    public String getDrillDownTypeForService(final MultivaluedMap<String, String> requestParameters) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AggregationTableInfo getAggregationView(final String type) {
        return new AggregationTableInfo("TAC_CG_EC");
    }

    @Override
    public List<String> getApplicableTechPacks(final MultivaluedMap<String, String> requestParameters) {
        final List<String> techPacks = new ArrayList<String>();
        techPacks.add(EVENT_E_GSM_CFA);
        return techPacks;
    }

    @Override
    public boolean areRawTablesRequiredForAggregationQueries() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getMaxAllowableSize() {
        return MAXIMUM_POSSIBLE_CONFIGURABLE_MAX_JSON_RESULT_SIZE;
    }

    @Override
    public boolean requiredToCheckValidParameterValue(final MultivaluedMap<String, String> requestParameters) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getTableSuffixKey() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<String> getMeasurementTypes() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<String> getRawTableKeys() {
        final List<String> rawTableKeys = new ArrayList<String>();
        rawTableKeys.add(ERR);
        return rawTableKeys;
    }

}
