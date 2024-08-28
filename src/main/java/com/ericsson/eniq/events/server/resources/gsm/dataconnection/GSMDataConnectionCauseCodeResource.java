/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.gsm.dataconnection;

import static com.ericsson.eniq.events.server.resources.gsm.GSMApplicationConstants.*;

import javax.ejb.EJB;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import com.ericsson.eniq.events.server.common.MediaTypeConstants;
import com.ericsson.eniq.events.server.resources.AbstractResource;
import com.ericsson.eniq.events.server.serviceprovider.Service;

/**
 * @author ejoegaf
 * @since 2012
 *
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class GSMDataConnectionCauseCodeResource extends AbstractResource {

    private static final String GSM_DATA_CONN_SUBSCRIBER_CAUSE_CODE_ANALYSIS_SERVICE = "SubscriberCCService";

    private static final String GSM_DATA_CONN_SUBSCRIBER_SUB_CAUSE_CODE_ANALYSIS_SERVICE = "SubscriberSCCService";

    private static final String GSM_DATA_CONN_SUBSCRIBER_CAUSE_CODE_LIST_SERVICE = "SubscriberCCListService";

    private static final String GSM_DATA_CONN_SUBSCRIBER_SUCCESS_CAUSE_CODE_SERVICE = "SubscriberSuccessCCService";

    @EJB(beanName = GSM_DATA_CONN_SUBSCRIBER_CAUSE_CODE_ANALYSIS_SERVICE)
    private Service gsmDataConnectionSubscriberCauseCodeService;

    @EJB(beanName = GSM_DATA_CONN_SUBSCRIBER_SUB_CAUSE_CODE_ANALYSIS_SERVICE)
    private Service gsmDataConnectionSubscriberSubCauseCodeService;

    @EJB(beanName = GSM_DATA_CONN_SUBSCRIBER_CAUSE_CODE_LIST_SERVICE)
    private Service gsmDataConnectionSubscriberCauseCodeListService;

    @EJB(beanName = GSM_DATA_CONN_SUBSCRIBER_SUCCESS_CAUSE_CODE_SERVICE)
    private Service gsmDataConnectionSubscriberSuccessCauseCodeService;

    @Path(CC_LIST_IMSI)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getIMSIiCauseCodeList() {
        final MultivaluedMap<String, String> reqParams = mapResourceLayerParameters();
        return gsmDataConnectionSubscriberCauseCodeListService.getData(reqParams);
    }

    @Path(CC_LIST_IMSI)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getIMSICauseCodeListAsCSV() {
        throw new UnsupportedOperationException();
    }

    @Path(CAUSE_CODE_PIE_CHART_IMSI)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getIMSICauseCodePieChart() {
        final MultivaluedMap<String, String> reqParams = mapResourceLayerParameters();

        return gsmDataConnectionSubscriberCauseCodeService.getData(reqParams);
    }

    @Path(CAUSE_CODE_PIE_CHART_IMSI)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getIMSICauseCodePieChartAsCSV() {
        throw new UnsupportedOperationException();
    }

    @Path(SUB_CAUSE_CODE_PIE_CHART_IMSI)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getIMSISubCauseCodePieChart() {
        final MultivaluedMap<String, String> reqParams = mapResourceLayerParameters();

        return gsmDataConnectionSubscriberSubCauseCodeService.getData(reqParams);
    }

    @Path(SUB_CAUSE_CODE_PIE_CHART_IMSI)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getIMSISubCauseCodePieChartAsCSV() {
        throw new UnsupportedOperationException();
    }

    @Path(SUCCESS_CAUSE_CODE_PIE_CHART_IMSI)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getIMSISuccessCauseCodePieChart() {
        final MultivaluedMap<String, String> reqParams = mapResourceLayerParameters();

        return gsmDataConnectionSubscriberSuccessCauseCodeService.getData(reqParams);
    }

    @Path(SUCCESS_CAUSE_CODE_PIE_CHART_IMSI)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getIMSISuccessCauseCodePieChartAsCSV() {
        throw new UnsupportedOperationException();
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.AbstractResource#getService()
     */
    @Override
    protected Service getService() {
        throw new UnsupportedOperationException();
    }
}