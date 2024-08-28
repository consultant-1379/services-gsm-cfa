/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
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

import org.apache.commons.lang.math.NumberUtils;

import com.ericsson.eniq.events.server.common.TechPackList;
import com.ericsson.eniq.events.server.common.tablesandviews.AggregationTableInfo;
import com.ericsson.eniq.events.server.query.QueryParameter;
import com.ericsson.eniq.events.server.serviceprovider.Service;
import com.ericsson.eniq.events.server.serviceprovider.impl.GenericService;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ewanggu
 * @since 2011
 */
@Stateless
@Local(Service.class)
public class AccessAreaCCService extends GenericService {

    /*
     * (non-Javadoc)
     *
     * @see
     * com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface
     * #getTemplatePath()
     */
    @Override
    public String getTemplatePath() {
        return GSM_CALL_FAILURE_ACCESS_AREA_CAUSE_CODE_ANALYSIS;
    }

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
        if (requestParameters.containsKey(CAUSE_CODE_ID_LIST)) {
            serviceSpecificTemplateParameters.put(CAUSE_CODE_ID_LIST, requestParameters.getFirst(CAUSE_CODE_ID_LIST));
        }
        serviceSpecificTemplateParameters.put(DISPLAY_PARAM, requestParameters.getFirst(DISPLAY_PARAM));
        if (requestParameters.containsKey(NODE_PARAM)) {
            serviceSpecificTemplateParameters.put(NODE_PARAM, requestParameters.getFirst(NODE_PARAM));
        } else if (requestParameters.containsKey(CELL) && requestParameters.containsKey(FAILURE_TYPE_PARAM)) {
            serviceSpecificTemplateParameters.put(NODE_PARAM, requestParameters.getFirst(CELL) + " - "
                    + requestParameters.getFirst(FAILURE_TYPE_PARAM));
            serviceSpecificTemplateParameters.put(FAILURE_TYPE_PARAM, requestParameters.getFirst(FAILURE_TYPE_PARAM));
        }
        if (requestParameters.containsKey(CATEGORY_ID) && NumberUtils.isNumber(requestParameters.getFirst(CATEGORY_ID))) {
            serviceSpecificTemplateParameters.put(CATEGORY_ID, requestParameters.getFirst(CATEGORY_ID));
        } else {
            serviceSpecificTemplateParameters.put(CATEGORY_ID, "");
        }
        return serviceSpecificTemplateParameters;
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

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface
     * #getServiceSpecificQueryParameters(javax.ws.rs.core.MultivaluedMap)
     */
    @Override
    public Map<String, QueryParameter> getServiceSpecificQueryParameters(
            final MultivaluedMap<String, String> requestParameters) {
        final Map<String, QueryParameter> queryParameters = new HashMap<String, QueryParameter>();
        long hashCellId = 0;
        final String node = requestParameters.getFirst(NODE_PARAM);
        if (node != null) {
            final String[] allData = node.split(DELIMITER);
            if (allData != null && allData.length == 5) {
                hashCellId = getQueryUtils().createHashIDForCell(allData[4], allData[2], allData[1], allData[0],
                        allData[3]);
            }
        }
        if (hashCellId != 0) {
            queryParameters.put(CELL_SQL_ID, QueryParameter.createLongParameter(hashCellId));
        } else if (requestParameters.containsKey(CELL_SQL_ID)) {
            queryParameters.put(CELL_SQL_ID,
                    QueryParameter.createLongParameter(Long.parseLong(requestParameters.getFirst(CELL_SQL_ID))));
            if (requestParameters.containsKey(CATEGORY_ID)
                    && NumberUtils.isNumber(requestParameters.getFirst(CATEGORY_ID))) {
                queryParameters.put(CATEGORY_ID,
                        QueryParameter.createIntParameter(Integer.parseInt(requestParameters.getFirst(CATEGORY_ID))));
            }
        }

        return queryParameters;
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
        requiredParameters.add(TYPE_PARAM);
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
        final MultivaluedMap<String, String> staticParameters = new MultivaluedMapImpl();
        staticParameters.add(TYPE_PARAM, TYPE_CELL);
        return staticParameters;
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
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface#getAggregationView(java.lang.String)
     */
    @Override
    public AggregationTableInfo getAggregationView(final String type) {
        return new AggregationTableInfo(NO_AGGREGATION_AVAILABLE);
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
        return false;
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
        return MAXIMUM_POSSIBLE_CONFIGURABLE_MAX_JSON_RESULT_SIZE;
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
        // TODO Auto-generated method stub
        return false;
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

}
