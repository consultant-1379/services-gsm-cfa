package com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events;

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

/**
 * @author ewanggu
 * @since 2012
 */
@Stateless
@Local(Service.class)
public class CCSubCCSummaryService extends GenericService {

    /*
     * (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#
     * getServiceSpecificTemplateParameters(javax.ws.rs.core.MultivaluedMap,
     * com.
     * ericsson.eniq.events.server.utils.DateTimeRange.FormattedDateTimeRange)
     */
    @Override
    public Map<String, Object> getServiceSpecificTemplateParameters(
            final MultivaluedMap<String, String> requestParameters, final FormattedDateTimeRange dateTimeRange,
            final TechPackList techPackList) {
        final Map<String, Object> serviceSpecificTemplateParameters = new HashMap<String, Object>();
        serviceSpecificTemplateParameters.put(ERR_AGGREGATION_VIEW, techPackList.getTechPack(EVENT_E_GSM_CFA)
                .getErrAggregationView());
        serviceSpecificTemplateParameters.put(CAUSE_CODE_DESCRIPTION,
                requestParameters.getFirst(CAUSE_CODE_DESCRIPTION));
        return serviceSpecificTemplateParameters;
    }

    /*
     * (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#
     * getTemplatePath()
     */
    @Override
    public String getTemplatePath() {
        return GSM_CALL_FAILURE_SUB_CAUSE_CODE_SUMMARY;
    }

    /*
     * (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#
     * getServiceSpecificDataServiceParameters(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    public Map<String, Object> getServiceSpecificDataServiceParameters(
            final MultivaluedMap<String, String> requestParameters) {
        final Map<String, Object> dataServiceParameters = new HashMap<String, Object>();
        dataServiceParameters.put(TZ_OFFSET, requestParameters.getFirst(TZ_OFFSET));
        return dataServiceParameters;
    }

    /*
     * (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#
     * getRequiredParametersForQuery()
     */
    @Override
    public List<String> getRequiredParametersForQuery() {
        final List<String> requiredParameters = new ArrayList<String>();
        requiredParameters.add(TZ_OFFSET);
        requiredParameters.add(CAUSE_CODE_ID);
        requiredParameters.add(CATEGORY_ID);
        requiredParameters.add(CAUSE_CODE_DESCRIPTION);
        return requiredParameters;
    }

    /*
     * (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#
     * getValidDisplayParameters()
     */
    @Override
    public MultivaluedMap<String, String> getStaticParameters() {
        return new MultivaluedMapImpl();
    }

    /*
     * (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#
     * getDrillDownTypeForService()
     */
    @Override
    public String getDrillDownTypeForService(final MultivaluedMap<String, String> requestParameters) {
        return null;
    }

    /*
     * (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#
     * getAggregationViews()
     */
    @Override
    public AggregationTableInfo getAggregationView(final String type) {
        return new AggregationTableInfo("HIER3_UC_EC");
    }

    /*
     * (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#
     * getApplicableTechPacks(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    public List<String> getApplicableTechPacks(final MultivaluedMap<String, String> requestParameters) {
        final List<String> techPacks = new ArrayList<String>();
        techPacks.add(EVENT_E_GSM_CFA);
        return techPacks;
    }

    /*
     * (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#
     * areRawTablesRequiredForQuery()
     */
    @Override
    public boolean areRawTablesRequiredForAggregationQueries() {
        return true;
    }

    /*
     * (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#
     * getMaxAllowableSize()
     */
    @Override
    public int getMaxAllowableSize() {
        return MAXIMUM_POSSIBLE_CONFIGURABLE_MAX_JSON_RESULT_SIZE;
    }

    /*
     * (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#
     * requiredToCheckValidParameterValue(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    public boolean requiredToCheckValidParameterValue(final MultivaluedMap<String, String> requestParameters) {
        return false;
    }

    /*
     * (non-Javadoc)
     * @see
     * com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface
     * #getServiceSpecificQueryParameters(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    public Map<String, QueryParameter> getServiceSpecificQueryParameters(
            final MultivaluedMap<String, String> requestParameters) {
        final Map<String, QueryParameter> queryParameters = new HashMap<String, QueryParameter>();
        queryParameters.put(CATEGORY_ID,
                QueryParameter.createIntParameter(Integer.parseInt(requestParameters.getFirst(CATEGORY_ID))));
        return queryParameters;
    }

    /*
     * The default scenario is that there are no raw table keys.
     * If they are needed overwrite this method in your Service class
     * @return
     */
    @Override
    public List<String> getRawTableKeys() {
        final List<String> rawTableKeys = new ArrayList<String>();
        rawTableKeys.add(ERR);
        return rawTableKeys;
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface#getTableSuffixKey()
     */
    @Override
    public String getTableSuffixKey() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface#getMeasurementTypes()
     */
    @Override
    public List<String> getMeasurementTypes() {
        // TODO Auto-generated method stub
        return null;
    }
}
