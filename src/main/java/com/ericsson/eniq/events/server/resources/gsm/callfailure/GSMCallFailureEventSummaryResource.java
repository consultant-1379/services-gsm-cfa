/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.gsm.callfailure;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.resources.gsm.GSMApplicationConstants.*;

import javax.ejb.EJB;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import com.ericsson.eniq.events.server.common.MediaTypeConstants;
import com.ericsson.eniq.events.server.resources.AbstractResource;
import com.ericsson.eniq.events.server.serviceprovider.Service;

/**
 * @author ehorpte
 * @since 2011
 *
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class GSMCallFailureEventSummaryResource extends AbstractResource {

    private static final String GSM_CFA_ACCES_AREA_EVENT_SUMMARY_SERVICE = "AccessAreaSummaryService";

    private static final String GSM_CFA_BSC_EVENT_SUMMARY_SERVICE = "ControllerSummaryService";

    private static final String GSM_CFA_SUB_CAUSE_CODE_SUMMARY_SERVICE = "CCSubCCSummaryService";

    private static final String GSM_CFA_IMSI_EVENT_SUMMARY_SERVICE = "SubscriberSummaryService";

    private static final String GSM_CFA_MSISDN_EVENT_SUMMARY_SERVICE = "MSISDNSubscriberSummaryService";

    private static final String GSM_CFA_BSC_GROUP_EVENT_SUMMARY_SERVICE = "ControllerGroupEventAnalysisService";

    private static final String GSM_IMSI_GROUP_SUMMARY_BREAKDOWN_SERVICE = "SubscriberGroupBreakdownService";

    private static final String GSM_CFA_ACCES_AREA_GROUP_EVENT_SUMMARY_SERVICE = "AccessAreaGroupSummaryService";

    private static final String GSM_CFA_TERMINAL_SUMMARY_SERVICE = "TerminalSummaryService";

    private static final String GSM_CFA_ACCESS_AREA_DISTRIBUTION_SUMMARY_SERVICE = "AccessAreaDistributionSummaryService";

    @EJB(beanName = GSM_CFA_ACCES_AREA_EVENT_SUMMARY_SERVICE)
    private Service gsmAccessAreaCallFailureEventSummaryService;

    @EJB(beanName = GSM_CFA_BSC_EVENT_SUMMARY_SERVICE)
    private Service gsmBscCallFailureEventSummaryService;

    @EJB(beanName = GSM_CFA_SUB_CAUSE_CODE_SUMMARY_SERVICE)
    private Service gsmCFASubCauseCodeEventSummaryService;

    @EJB(beanName = GSM_CFA_IMSI_EVENT_SUMMARY_SERVICE)
    private Service gsmCfaImsiEventSummaryService;

    @EJB(beanName = GSM_CFA_MSISDN_EVENT_SUMMARY_SERVICE)
    private Service gsmCfaMsisdnEventSummaryService;

    @EJB(beanName = GSM_CFA_BSC_GROUP_EVENT_SUMMARY_SERVICE)
    private Service gsmBscGroupCallFailureEventSummaryService;

    @EJB(beanName = GSM_IMSI_GROUP_SUMMARY_BREAKDOWN_SERVICE)
    private Service gsmSubscriberGroupBreakdownService;

    @EJB(beanName = GSM_CFA_ACCES_AREA_GROUP_EVENT_SUMMARY_SERVICE)
    private Service gsmAccessAreaGroupCallFailureEventSummaryService;

    @EJB(beanName = GSM_CFA_TERMINAL_SUMMARY_SERVICE)
    private Service gsmTerminalSummaryService;

    @EJB(beanName = GSM_CFA_ACCESS_AREA_DISTRIBUTION_SUMMARY_SERVICE)
    private Service gsmAccessAreaDistributionSummaryService;

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
    public String getCallFailureEventSummaryAccessArea() {
        return gsmAccessAreaCallFailureEventSummaryService.getData(mapResourceLayerParameters());
    }

    @Path(ACCESS_AREA)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallFailureEventSummaryAccessAreaAsCSV() {
        return gsmAccessAreaCallFailureEventSummaryService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(BSC)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCallFailureEventSummaryBsc() {
        return gsmBscCallFailureEventSummaryService.getData(mapResourceLayerParameters());
    }

    @Path(BSC)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallFailureEventSummaryAccessBscCSV() {
        return gsmBscCallFailureEventSummaryService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(BSC_GROUP_PATH)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCallFailureEventSummaryBscGroup() {
        return gsmBscGroupCallFailureEventSummaryService.getData(mapResourceLayerParameters());
    }

    @Path(BSC_GROUP_PATH)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallFailureEventSummaryBscGroupCSV() {
        return gsmBscGroupCallFailureEventSummaryService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(NODE)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getNodeDetailedEventAnalysis() {
        final MultivaluedMap<String, String> reqParams = mapResourceLayerParameters();
        final String type = reqParams.getFirst(TYPE_PARAM);
        if (TYPE_BSC.equals(type)) {
            return gsmBscCallFailureEventSummaryService.getData(reqParams);
        }
        return gsmAccessAreaCallFailureEventSummaryService.getData(reqParams);
    }

    @Path(NODE)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getNodeDetailedEventAnalysisAsCSV() {
        final MultivaluedMap<String, String> reqParams = mapResourceLayerParameters();
        final String type = reqParams.getFirst(TYPE_PARAM);
        if (TYPE_BSC.equals(type)) {
            return gsmBscCallFailureEventSummaryService.getDataAsCSV(mapResourceLayerParameters(), response);
        }
        return gsmAccessAreaCallFailureEventSummaryService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(EXTENDED_CAUSE)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCallFailureEventSummarySubCauseCode() {
        return gsmCFASubCauseCodeEventSummaryService.getData(mapResourceLayerParameters());
    }

    @Path(EXTENDED_CAUSE)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallFailureEventSummarySubCauseCodeAsCSV() {
        return gsmCFASubCauseCodeEventSummaryService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(IMSI)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCallFailureEventSummaryImsi() {

        final MultivaluedMap<String, String> reqParams = mapResourceLayerParameters();
        final String type = reqParams.getFirst(TYPE_PARAM);
        if (MSISDN_PARAM_UPPER_CASE.equals(type)) {
            return gsmCfaMsisdnEventSummaryService.getData(reqParams);
        }

        return gsmCfaImsiEventSummaryService.getData(mapResourceLayerParameters());
    }

    @Path(IMSI)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallFailureEventSummaryAccessImsiCSV() {

        final MultivaluedMap<String, String> reqParams = mapResourceLayerParameters();
        final String type = reqParams.getFirst(TYPE_PARAM);
        if (MSISDN_PARAM_UPPER_CASE.equals(type)) {
            return gsmCfaMsisdnEventSummaryService.getDataAsCSV(reqParams, response);
        }
        return gsmCfaImsiEventSummaryService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(IMSI_GROUP_BREAKDOWN)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCallFailureEventSummaryImsiGroupBreakDown() {
        return gsmSubscriberGroupBreakdownService.getData(mapResourceLayerParameters());
    }

    @Path(IMSI_GROUP_BREAKDOWN)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallFailureEventSummaryImsiGroupBreakDownCSV() {
        return gsmSubscriberGroupBreakdownService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(ACCESS_AREA_GROUP)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCallFailureEventSummaryAccessAreaGroup() {
        return gsmAccessAreaGroupCallFailureEventSummaryService.getData(mapResourceLayerParameters());
    }

    @Path(ACCESS_AREA_GROUP)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallFailureEventSummaryAccessAreaGroupAsCSV() {
        return gsmAccessAreaGroupCallFailureEventSummaryService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(TERMINAL_SERVICES)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallFailureSummaryTerminalAsCSV() {
        return gsmTerminalSummaryService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(TERMINAL_SERVICES)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCallFailureEventSummaryTerminal() {
        return gsmTerminalSummaryService.getData(mapResourceLayerParameters());
    }

    @Path(ACCESS_AREA_DISTRIBUTION)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallFailureSummaryDisCellAsCSV() {
        return gsmAccessAreaDistributionSummaryService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(ACCESS_AREA_DISTRIBUTION)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCallFailureEventSummaryDisCell() {
        return gsmAccessAreaDistributionSummaryService.getData(mapResourceLayerParameters());
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
     * @see com.ericsson.eniq.events.server.resources.AbstractResource#getDataAsCSV()
     */
    @Override
    public Response getDataAsCSV() throws WebApplicationException {
        throw new UnsupportedOperationException();
    }

}
