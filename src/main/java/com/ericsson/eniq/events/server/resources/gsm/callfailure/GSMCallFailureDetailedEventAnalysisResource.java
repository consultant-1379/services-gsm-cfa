/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2014
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
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
 * @since 2011
 *
 */
@Stateless
@LocalBean
public class GSMCallFailureDetailedEventAnalysisResource extends AbstractResource {

    private static final String GSM_ACCESS_AREA_DETAILED_EVENT_ANALYSIS_SERVICE = "AccessAreaDetailedService";

    private static final String GSM_CAUSE_CODE_DETAILED_EVENT_ANALYSIS_SERVICE = "CCSubCCDetailedService";

    private static final String GSM_BSC_DETAILED_EVENT_ANALYSIS_SERVICE = "ControllerDetailedService";

    private static final String GSM_SUBSRIBER_DETAILED_EVENT_ANALYSIS_SERVICE = "CallFailureSubscriberDetailedService";

    private static final String GSM_TERMINAL_DETAILED_EVENT_ANALYSIS_SERVICE = "TerminalDetailedService";

    @EJB(beanName = GSM_ACCESS_AREA_DETAILED_EVENT_ANALYSIS_SERVICE)
    private Service gsmCallFailureAccessAreaDetailedEventAnalysisService;

    @EJB(beanName = GSM_CAUSE_CODE_DETAILED_EVENT_ANALYSIS_SERVICE)
    private Service gsmCallFailureCallDropsDetailedEventAnalysisService;

    @EJB(beanName = GSM_BSC_DETAILED_EVENT_ANALYSIS_SERVICE)
    private Service gsmCallFailureBscDetailedEventAnalysisService;

    @EJB(beanName = GSM_SUBSRIBER_DETAILED_EVENT_ANALYSIS_SERVICE)
    private Service gsmCallFailureSubscriberDetailedEventAnalysisService;

    @EJB(beanName = GSM_TERMINAL_DETAILED_EVENT_ANALYSIS_SERVICE)
    private Service gsmCallFailureTerminalDetailedEventAnalysisService;

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.AbstractResource#getService()
     */
    @Override
    protected Service getService() {
        throw new UnsupportedOperationException();
    }

    @Path(ACCESS_AREA)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCallFailureEventDetailedAccessArea() {
        return gsmCallFailureAccessAreaDetailedEventAnalysisService.getData(mapResourceLayerParameters());
    }

    @Path(ACCESS_AREA)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallFailureEventDetailedAccessAreaAsCSV() {
        return gsmCallFailureAccessAreaDetailedEventAnalysisService
                .getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(BSC)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getBscDetailedEventAnalysis() {
        return gsmCallFailureBscDetailedEventAnalysisService.getData(mapResourceLayerParameters());
    }

    @Path(BSC)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getBscDetailedEventAnalysisAsCSV() {
        return gsmCallFailureBscDetailedEventAnalysisService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(CALL_FAILURE_RANKING_BY_CALLS_DROPPED_URI)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCallDropsDetailedEventAnalysisCC() {
        return gsmCallFailureCallDropsDetailedEventAnalysisService.getData(mapResourceLayerParameters());
    }

    @Path(CALL_FAILURE_RANKING_BY_CALLS_DROPPED_URI)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallDropsDetailedEventAnalysisCCAsCSV() {
        return gsmCallFailureCallDropsDetailedEventAnalysisService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(SUBSCRIBER_FOR_CALL_DROP)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getSubscriberCallDropDetailedEventAnalysis() {
        return gsmCallFailureSubscriberDetailedEventAnalysisService.getData(mapResourceLayerParameters());
    }

    @Path(SUBSCRIBER_FOR_CALL_DROP)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getSubscriberCallDropDetailedEventAnalysisAsCSV() {
        return gsmCallFailureSubscriberDetailedEventAnalysisService
                .getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(SUBSCRIBER_FOR_CALL_SETUP_FAILURE)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getSubscriberCallSetupDetailedEventAnalysis() {
        return gsmCallFailureSubscriberDetailedEventAnalysisService.getData(mapResourceLayerParameters());
    }

    @Path(SUBSCRIBER_FOR_CALL_SETUP_FAILURE)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getSubscriberCallSetupDetailedEventAnalysisAsCSV() {
        return gsmCallFailureSubscriberDetailedEventAnalysisService
                .getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(TAC)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getTerminalDetailedEventAnalysis() {
        return gsmCallFailureTerminalDetailedEventAnalysisService.getData(mapResourceLayerParameters());
    }

    @Path(TAC)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getTerminalDetailedEventAnalysisAsCSV() {
        return gsmCallFailureTerminalDetailedEventAnalysisService.getDataAsCSV(mapResourceLayerParameters(), response);
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
