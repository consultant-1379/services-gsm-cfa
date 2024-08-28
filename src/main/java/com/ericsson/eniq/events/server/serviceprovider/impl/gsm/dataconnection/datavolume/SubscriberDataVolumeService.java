/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.serviceprovider.impl.gsm.dataconnection.datavolume;

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
 * @author ETHOMIT
 * @since 2012
 * 
 */
@Stateless
@Local(Service.class)
public class SubscriberDataVolumeService extends GenericService {

    /*
     * (non-Javadoc)
     * 
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#
     * getTemplatePath()
     */
    @Override
    public String getTemplatePath() {
        return GSM_DATA_CONNECTION_SUBSCRIBER_DATA_VOLUME_ANALYSIS;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#
     * getServiceSpecificTemplateParameters(javax.ws.rs.core.MultivaluedMap,
     * com.
     * ericsson.eniq.events.server.utils.DateTimeRange.FormattedDateTimeRange)
     */
    @Override
    public Map<String, Object> getServiceSpecificTemplateParameters(
            final MultivaluedMap<String, String> requestParameters, final FormattedDateTimeRange dateTimeRange,
            final TechPackList techPackList) {
        return new HashMap<String, Object>();
    }

    /*
     * (non-Javadoc)
     * 
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
     * 
     * @see
     * com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface
     * #getServiceSpecificQueryParameters(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    public Map<String, QueryParameter> getServiceSpecificQueryParameters(
            final MultivaluedMap<String, String> requestParameters) {
        final Map<String, QueryParameter> queryParameters = new HashMap<String, QueryParameter>();

        if (requestParameters.containsKey(IMSI_PARAM_UPPER_CASE)) {
            queryParameters.put(
                    IMSI_PARAM_UPPER_CASE,
                    getQueryUtils().createQueryParameter(IMSI_PARAM_UPPER_CASE,
                            requestParameters.getFirst(IMSI_PARAM_UPPER_CASE)));
        } else if (requestParameters.containsKey(IMSI_PARAM)) {
            queryParameters
                    .put(IMSI_PARAM_UPPER_CASE,
                            getQueryUtils().createQueryParameter(IMSI_PARAM_UPPER_CASE,
                                    requestParameters.getFirst(IMSI_PARAM)));
        }

        if (requestParameters.containsKey(GROUP_NAME_PARAM)) {
            queryParameters.put(GROUP_NAME_PARAM,
                    getQueryUtils()
                            .createQueryParameter(GROUP_NAME_PARAM, requestParameters.getFirst(GROUP_NAME_PARAM)));
        }

        return queryParameters;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#
     * getRequiredParametersForQuery()
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
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#
     * getDrillDownTypeForService()
     */
    @Override
    public String getDrillDownTypeForService(final MultivaluedMap<String, String> requestParameters) {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#
     * getAggregationViews()
     */
    @Override
    public AggregationTableInfo getAggregationView(final String type) {
        return new AggregationTableInfo(NO_AGGREGATION_AVAILABLE);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#
     * getApplicableTechPacks(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    public List<String> getApplicableTechPacks(final MultivaluedMap<String, String> requestParameters) {
        final List<String> techPacks = new ArrayList<String>();
        techPacks.add(EVENT_E_GSM_PS);
        return techPacks;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#
     * areRawTablesRequiredForQuery()
     */
    @Override
    public boolean areRawTablesRequiredForAggregationQueries() {
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#
     * getMaxAllowableSize()
     */
    @Override
    public int getMaxAllowableSize() {
        return MAXIMUM_POSSIBLE_CONFIGURABLE_MAX_JSON_RESULT_SIZE;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#
     * requiredToCheckValidParameterValue(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    public boolean requiredToCheckValidParameterValue(final MultivaluedMap<String, String> requestParameters) {
        if (requestParameters.containsKey(IMSI_PARAM) || requestParameters.containsKey(GROUP_NAME_PARAM)) {
            return true;
        }
        return false;

    }

    /*
     * The default scenario is that there are no raw table keys. If they are
     * needed overwrite this method in your Service class
     * 
     * @return
     */
    @Override
    public List<String> getRawTableKeys() {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface
     * #getTableSuffixKey()
     */
    @Override
    public String getTableSuffixKey() {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface
     * #getMeasurementTypes()
     */
    @Override
    public List<String> getMeasurementTypes() {
        final List<String> measurementTypeKeys = new ArrayList<String>();
        measurementTypeKeys.add(GSM_PRE_AGG_TABLE_KEY);
        return measurementTypeKeys;
    }

}