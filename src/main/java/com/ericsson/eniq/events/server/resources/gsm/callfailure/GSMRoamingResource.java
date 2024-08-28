/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.gsm.callfailure;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import javax.ejb.EJB;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.ericsson.eniq.events.server.common.MediaTypeConstants;
import com.ericsson.eniq.events.server.resources.AbstractResource;
import com.ericsson.eniq.events.server.serviceprovider.Service;

/**
 * @author eatiaro
 * 2012
 *
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class GSMRoamingResource extends AbstractResource {

    private static final String GSM_CFA_OPERATOR_ROAMING_SERVICE = "OperatorRoamingSummaryService";

    private static final String GSM_CFA_COUNTRY_ROAMING_SERVICE = "CountryRoamingSummaryService";

    private static final String GSM_CFA_COUNTRY_DRILL_ROAMING_SERVICE = "CountryCallFailureSummaryService";

    private static final String GSM_CFA_COUNTRY_ROAMING_DETAIL_SERVICE = "CountryRoamingDetailService";

    private static final String GSM_CFA_OPERATOR_DRILL_ROAMING_ANALYSIS_SERVICE = "OperatorDrillRoamingAnalysisService";

    private static final String GSM_CFA_OPERATOR_ROAMING_DETAIL_SERVICE = "OperatorRoamingDetailService";

    @EJB(beanName = GSM_CFA_OPERATOR_ROAMING_SERVICE)
    private Service gsmOpertorRoamingService;

    @EJB(beanName = GSM_CFA_COUNTRY_ROAMING_SERVICE)
    private Service gsmCountryRoamingService;

    @EJB(beanName = GSM_CFA_COUNTRY_DRILL_ROAMING_SERVICE)
    private Service gsmCountryCallFailureSummaryService;

    @EJB(beanName = GSM_CFA_COUNTRY_ROAMING_DETAIL_SERVICE)
    private Service gsmCountryRoamingDetailService;

    @EJB(beanName = GSM_CFA_OPERATOR_DRILL_ROAMING_ANALYSIS_SERVICE)
    private Service OperatorDrillRoamingAnalysisService;

    @EJB(beanName = GSM_CFA_OPERATOR_ROAMING_DETAIL_SERVICE)
    private Service gsmOperatorRoamingDetailService;

    @Path(OPERATOR)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getOperatorRoamingSummaryData() {
        return gsmOpertorRoamingService.getData(mapResourceLayerParameters());
    }

    @Path(OPERATOR)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getOperatorRoamingSummaryDataAsCSV() {
        return gsmOpertorRoamingService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(COUNTRY)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCountryRoamingSummaryData() {
        return gsmCountryRoamingService.getData(mapResourceLayerParameters());
    }

    @Path(COUNTRY)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCountryRoamingSummaryDataAsCSV() {
        return gsmCountryRoamingService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(ROAMING_COUNTRY_DRILL)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getRoamingDrillByCountryData() {
        return gsmCountryCallFailureSummaryService.getData(mapResourceLayerParameters());
    }

    @Path(ROAMING_COUNTRY_DRILL)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getRoamingDrillByCountryDataAsCSV() {
        return gsmCountryCallFailureSummaryService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(COUNTRY_DRILL_DETAIL_FOR_CALL_SETUP)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCallSetupCountryRoamingDetailData() {
        return gsmCountryRoamingDetailService.getData(mapResourceLayerParameters());
    }

    @Path(COUNTRY_DRILL_DETAIL_FOR_CALL_SETUP)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallSetupCountryRoamingDetailDataAsCSV() {
        return gsmCountryRoamingDetailService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(COUNTRY_DRILL_DETAIL_FOR_CALL_DROP)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCallDropCountryRoamingDetailData() {
        return gsmCountryRoamingDetailService.getData(mapResourceLayerParameters());
    }

    @Path(COUNTRY_DRILL_DETAIL_FOR_CALL_DROP)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallDropCountryRoamingDetailDataAsCSV() {
        return gsmCountryRoamingDetailService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(ROAMING_OPERATOR_DRILL)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getRoamingDrillByOperator() {
        return OperatorDrillRoamingAnalysisService.getData(mapResourceLayerParameters());
    }

    @Path(ROAMING_OPERATOR_DRILL)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getRoamingDrillByOperatorAsCSV() {
        return OperatorDrillRoamingAnalysisService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(OPERATOR_DRILL_DETAIL_FOR_CALL_SETUP)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCallSetupOperatorRoamingDetailData() {
        return gsmOperatorRoamingDetailService.getData(mapResourceLayerParameters());
    }

    @Path(OPERATOR_DRILL_DETAIL_FOR_CALL_SETUP)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallSetupOperatorRoamingDetailDataAsCSV() {
        return gsmOperatorRoamingDetailService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(OPERATOR_DRILL_DETAIL_FOR_CALL_DROP)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCallDropOperatorRoamingDetailData() {
        return gsmOperatorRoamingDetailService.getData(mapResourceLayerParameters());
    }

    @Path(OPERATOR_DRILL_DETAIL_FOR_CALL_DROP)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallDropOperatorRoamingDetailDataAsCSV() {
        return gsmOperatorRoamingDetailService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Override
    protected Service getService() {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.AbstractResource#getData()
     */
    @Override
    public String getData() throws WebApplicationException {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * @see
     * com.ericsson.eniq.events.server.resources.AbstractResource#getDataAsCSV()
     */
    @Override
    public Response getDataAsCSV() throws WebApplicationException {
        throw new UnsupportedOperationException();
    }

}
