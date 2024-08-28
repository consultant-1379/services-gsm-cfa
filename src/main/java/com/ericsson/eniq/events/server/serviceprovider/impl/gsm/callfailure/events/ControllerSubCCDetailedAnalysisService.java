/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
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
 * @author eprjaya
 *
 */
@Stateless
@Local(Service.class)
public class ControllerSubCCDetailedAnalysisService extends GenericService {

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#getServiceSpecificTemplateParameters(javax.ws.rs.core.MultivaluedMap, com.ericsson.eniq.events.server.utils.DateTimeRange.FormattedDateTimeRange)
     */
    @Override
    public Map<String, Object> getServiceSpecificTemplateParameters(
            final MultivaluedMap<String, String> requestParameters, final FormattedDateTimeRange dateTimeRange,
            final TechPackList techPackList) {
        final Map<String, Object> serviceSpecificTemplateParameters = new HashMap<String, Object>();
        serviceSpecificTemplateParameters.put(GROUP_NAME_KEY, requestParameters.getFirst(GROUP_NAME_PARAM));
        serviceSpecificTemplateParameters.put(CAUSE_VALUE, requestParameters.getFirst(CAUSE_CODE_DESCRIPTION));
        serviceSpecificTemplateParameters.put(EXTENDED_CAUSE_VALUE,
                requestParameters.getFirst(SUB_CAUSE_CODE_DESCRIPTION));
        return serviceSpecificTemplateParameters;
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#getTemplatePath()
     */
    @Override
    public String getTemplatePath() {
        return GSM_DETALIED_EVENT_CFA_BY_SUB_CAUSE_CODE_ANALYSIS;
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#getServiceSpecificDataServiceParameters(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    public Map<String, Object> getServiceSpecificDataServiceParameters(
            final MultivaluedMap<String, String> requestParameters) {
        final Map<String, Object> dataServiceParameters = new HashMap<String, Object>();
        dataServiceParameters.put(TZ_OFFSET, requestParameters.getFirst(TZ_OFFSET));
        return dataServiceParameters;
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#getRequiredParametersForQuery()
     */
    @Override
    public List<String> getRequiredParametersForQuery() {
        final List<String> requiredParameters = new ArrayList<String>();
        requiredParameters.add(TZ_OFFSET);
        requiredParameters.add(CAUSE_CODE_ID);
        requiredParameters.add(SUB_CAUSE_CODE_PARAM);
        return requiredParameters;
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#getValidDisplayParameters()
     */
    @Override
    public MultivaluedMap<String, String> getStaticParameters() {
        return new MultivaluedMapImpl();
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#getDrillDownTypeForService()
     */
    @Override
    public String getDrillDownTypeForService(final MultivaluedMap<String, String> requestParameters) {
        return null;
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#getAggregationViews()
     */
    @Override
    public AggregationTableInfo getAggregationView(final String type) {
        return new AggregationTableInfo(NO_AGGREGATION_AVAILABLE);
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#getApplicableTechPacks(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    public List<String> getApplicableTechPacks(final MultivaluedMap<String, String> requestParameters) {
        final List<String> techPacks = new ArrayList<String>();
        techPacks.add(EVENT_E_GSM_CFA);
        return techPacks;
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#areRawTablesRequiredForQuery()
     */
    @Override
    public boolean areRawTablesRequiredForAggregationQueries() {
        return false;
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#getMaxAllowableSize()
     */
    @Override
    public int getMaxAllowableSize() {
        return MAXIMUM_POSSIBLE_CONFIGURABLE_MAX_JSON_RESULT_SIZE;
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#requiredToCheckValidParameterValue(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    public boolean requiredToCheckValidParameterValue(final MultivaluedMap<String, String> requestParameters) {
        return false;
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface#getServiceSpecificQueryParameters(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    public Map<String, QueryParameter> getServiceSpecificQueryParameters(
            final MultivaluedMap<String, String> requestParameters) {
        final Map<String, QueryParameter> queryParameters = new HashMap<String, QueryParameter>();
        if (requestParameters.containsKey(NODE_PARAM)) {
            final String node = requestParameters.getFirst(NODE_PARAM);
            long hashedIdForController = 0;
            if (node != null) {
                final String[] nodeComponents = node.split(DELIMITER);
                if (nodeComponents != null && nodeComponents.length == 3) {
                    hashedIdForController = getQueryUtils().createHashIDForController(nodeComponents[2],
                            nodeComponents[0], nodeComponents[1]);
                }
            }
            if (hashedIdForController != 0) {
                queryParameters.put(CONTROLLER_SQL_ID, QueryParameter.createLongParameter(hashedIdForController));
            } else if (requestParameters.containsKey(CONTROLLER_SQL_ID)) {
                queryParameters.put(CONTROLLER_SQL_ID, QueryParameter.createLongParameter(Long
                        .parseLong(requestParameters.getFirst(CONTROLLER_SQL_ID))));
            }
        }
        final String bscGroupName = requestParameters.getFirst(GROUP_NAME_PARAM);
        if (bscGroupName != null) {
            queryParameters.put(GROUP_NAME_KEY, QueryParameter.createStringParameter(bscGroupName));
            requestParameters.remove(GROUP_NAME_PARAM);
        }
        queryParameters.put(CAUSE_GROUP,
                QueryParameter.createIntParameter(Integer.parseInt(requestParameters.getFirst(CAUSE_CODE_ID))));
        requestParameters.remove(CAUSE_CODE);
        queryParameters.put(EXTENDED_CAUSE,
                QueryParameter.createIntParameter(Integer.parseInt(requestParameters.getFirst(SUB_CAUSE_CODE))));
        requestParameters.remove(SUB_CAUSE_CODE);
        return queryParameters;
    }

    /**
     * For EVENT_E_RAN_CFA raw tables we only have ERR tables.
     * @return
     */
    @Override
    public List<String> getRawTableKeys() {
        final List<String> rawTableKeys = new ArrayList<String>();
        rawTableKeys.add(KEY_TYPE_ERR);
        return rawTableKeys;
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface#getTableSuffixKey()
     */
    @Override
    public String getTableSuffixKey() {
        return null;
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface#getMeasurementTypes()
     */
    @Override
    public List<String> getMeasurementTypes() {
        return null;
    }

    /*
     * (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#getTimeColumnIndices()
     */
    @Override
    public List<Integer> getTimeColumnIndices() {
        final List<Integer> columnIndices = new ArrayList<Integer>();
        columnIndices.add(1);
        return columnIndices;
    }

}
