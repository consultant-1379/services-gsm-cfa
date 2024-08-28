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

import com.ericsson.eniq.events.server.common.EventDataSourceType;
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
public class TerminalSummaryService extends GenericService {

    /*
     * (non-Javadoc)
     *
     * @see
     * com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface
     * #getServiceSpecificTemplateParameters(javax.ws.rs.core.MultivaluedMap,
     * com
     * .ericsson.eniq.events.server.utils.DateTimeRange.FormattedDateTimeRange)
     */
    @Override
    public Map<String, Object> getServiceSpecificTemplateParameters(
            final MultivaluedMap<String, String> requestParameters, final FormattedDateTimeRange dateTimeRange,
            final TechPackList techPackList) {
        final Map<String, Object> serviceSpecificTemplateParameters = new HashMap<String, Object>();
        serviceSpecificTemplateParameters.put(ERR_AGGREGATION_VIEW, techPackList.getTechPack(EVENT_E_GSM_CFA)
                .getErrAggregationView());
        serviceSpecificTemplateParameters.put(SUC_AGGREGATION_VIEW, techPackList.getTechPack(EVENT_E_GSM_CFA)
                .getSucAggregationView());
        return serviceSpecificTemplateParameters;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface
     * #getTemplatePath()
     */
    @Override
    public String getTemplatePath() {
        return GSM_CALL_FAILURE_TERMINAL_EVENT_ANALYSIS_SUMMARY;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface
     * #getServiceSpecificDataServiceParameters(javax.ws.rs.core.MultivaluedMap)
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
     *
     * @see
     * com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface
     * #getRequiredParametersForQuery()
     */
    @Override
    public List<String> getRequiredParametersForQuery() {
        final List<String> requiredParameters = new ArrayList<String>();
        requiredParameters.add(TZ_OFFSET);
        return requiredParameters;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface
     * #getStaticParameters()
     */
    @Override
    public MultivaluedMap<String, String> getStaticParameters() {
        return new MultivaluedMapImpl();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface
     * #getDrillDownTypeForService()
     */
    @Override
    public String getDrillDownTypeForService(final MultivaluedMap<String, String> requestParameters) {
        return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface
     * #getApplicableTechPacks(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    public List<String> getApplicableTechPacks(final MultivaluedMap<String, String> requestParameters) {
        final List<String> techPacks = new ArrayList<String>();
        techPacks.add(EVENT_E_GSM_CFA);
        return techPacks;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface
     * #areRawTablesRequiredForQuery()
     */
    @Override
    public boolean areRawTablesRequiredForAggregationQueries() {
        return true;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface
     * #getMaxAllowableSize()
     */
    @Override
    public int getMaxAllowableSize() {
        return DEFAULT_MAXIMUM_JSON_RESULT_SIZE;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface
     * #requiredToCheckValidParameterValue(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    public boolean requiredToCheckValidParameterValue(final MultivaluedMap<String, String> requestParameters) {
        if (requestParameters.containsKey(NODE_PARAM) || requestParameters.containsKey(GROUP_NAME_PARAM)) {
            return true;
        }
        return false;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface
     * #getAggregationView(java.lang.String)
     */
    @Override
    public AggregationTableInfo getAggregationView(final String type) {
        return new AggregationTableInfo("TAC_EVENTID", EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY);
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.servicepreeovider.impl.GenericServiceInterface
     * #getServiceSpecificQueryParameters(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    public Map<String, QueryParameter> getServiceSpecificQueryParameters(
            final MultivaluedMap<String, String> requestParameters) {
        final Map<String, QueryParameter> queryParameters = new HashMap<String, QueryParameter>();
        final String node = requestParameters.getFirst(NODE_PARAM);
        if (node != null) {
            final String[] allData = node.split(DELIMITER);
            if (allData != null) {
                queryParameters.put(TAC,
                        QueryParameter.createIntParameter(Integer.valueOf(allData[allData.length - 1])));
            }
        } else if (requestParameters.containsKey(TAC)) {
            queryParameters.put(TAC,
                    QueryParameter.createIntParameter(Integer.parseInt(requestParameters.getFirst(TAC))));
        }

        if (!queryParameters.containsKey(TAC) && !queryParameters.containsKey(TAC_PARAM)) {
            queryParameters.put(TAC, QueryParameter.createIntParameter(0));
        }
        return queryParameters;
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
     * The default scenario is that there are no raw table keys.
     * If they are needed overwrite this method in your Service class
     * @return
     */
    @Override
    public List<String> getRawTableKeys() {
        final List<String> rawTableKeys = new ArrayList<String>();
        rawTableKeys.add(ERR);
        rawTableKeys.add(SUC);
        return rawTableKeys;
    }
}
