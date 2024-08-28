/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.events;

import static com.ericsson.eniq.events.server.common.ApplicationConfigConstants.MAXIMUM_POSSIBLE_CONFIGURABLE_MAX_JSON_RESULT_SIZE;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.CATEGORY_ID;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.ERR;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.GSM_CALL_FAILURE_TERMINAL_DETAILED_EVENT_ANALYSIS;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.NO_AGGREGATION_AVAILABLE;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TAC;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TAC_PARAM;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.TZ_OFFSET;
import static com.ericsson.eniq.events.server.common.TechPackData.EVENT_E_GSM_CFA;
import static com.ericsson.eniq.events.server.resources.gsm.GSMApplicationConstants.CAUSE_GROUP;
import static com.ericsson.eniq.events.server.resources.gsm.GSMApplicationConstants.EXTENDED_CAUSE;

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
 * @author ejoegaf
 * @since 2011
 */
@Stateless
@Local(Service.class)
public class TerminalDetailedService extends GenericService {

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ericsson.eniq.events.server.serviceprovider.impl.GenericServiceInterface
	 * #getTemplatePath()
	 */
	@Override
	public String getTemplatePath() {
		return GSM_CALL_FAILURE_TERMINAL_DETAILED_EVENT_ANALYSIS;
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
			final MultivaluedMap<String, String> requestParameters,
			final FormattedDateTimeRange dateTimeRange,
			final TechPackList techPackList) {
		final Map<String, Object> serviceSpecificTemplateParameters = new HashMap<String, Object>();
		serviceSpecificTemplateParameters.put(CATEGORY_ID,
				requestParameters.getFirst(CATEGORY_ID));
		serviceSpecificTemplateParameters.put(CAUSE_GROUP,
				requestParameters.getFirst(CAUSE_GROUP));
		serviceSpecificTemplateParameters.put(EXTENDED_CAUSE,
				requestParameters.getFirst(EXTENDED_CAUSE));
		if (requestParameters.getFirst(TAC_PARAM) != null) {
			serviceSpecificTemplateParameters.put(TAC,
					requestParameters.getFirst(TAC_PARAM));
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
		dataServiceParameters.put(TZ_OFFSET,
				requestParameters.getFirst(TZ_OFFSET));
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
		final String tac = requestParameters.getFirst(TAC);
		queryParameters.put(TAC,
				QueryParameter.createIntParameter(Integer.parseInt(tac)));
		final String category_id = requestParameters.getFirst(CATEGORY_ID);
		queryParameters.put(CATEGORY_ID, QueryParameter
				.createIntParameter(Integer.parseInt(category_id)));
		queryParameters
				.put(CAUSE_GROUP, QueryParameter
						.createStringParameter(requestParameters
								.getFirst(CAUSE_GROUP)));
		queryParameters.put(EXTENDED_CAUSE, QueryParameter
				.createStringParameter(requestParameters
						.getFirst(EXTENDED_CAUSE)));
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
		requiredParameters.add(TAC);
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
	public String getDrillDownTypeForService(
			final MultivaluedMap<String, String> requestParameters) {
		return null;
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
	public List<String> getApplicableTechPacks(
			final MultivaluedMap<String, String> requestParameters) {
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
	public boolean requiredToCheckValidParameterValue(
			final MultivaluedMap<String, String> requestParameters) {
		return false;
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
		return null;
	}

	/*
	 * ERR = raw table keys.
	 * 
	 * @return
	 */
	@Override
	public List<String> getRawTableKeys() {
		final List<String> rawTableKeys = new ArrayList<String>();
		rawTableKeys.add(ERR);
		return rawTableKeys;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ericsson.eniq.events.server.serviceprovider.impl.GenericService#
	 * getTimeColumnIndices()
	 */
	@Override
	public List<Integer> getTimeColumnIndices() {
		final List<Integer> columnIndices = new ArrayList<Integer>();
		columnIndices.add(1);
		return columnIndices;
	}

}
