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
 * @author ejoegaf
 *
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class GSMCallFailureRankingResource extends AbstractResource {

    private static final String GSM_CFA_ACCESS_AREA_RANKING_SERVICE = "AccessAreaRankingService";

    private static final String GSM_CFA_TERMINAL_RANKING_SERVICE = "TerminalRankingService";

    private static final String GSM_CAUSE_CODE_CALLS_DROPPED_FAILURE_RANKING_SERVICE = "CauseCodeRankingService";

    private static final String GSM_SUBSCRIBER_CALL_DROP_RANKING_SERVICE = "SubscriberRankingService";

    private static final String GSM_CFA_CONTROLLER_RANKING_SERVICE = "ControllerRankingService";

    @EJB(beanName = GSM_CFA_ACCESS_AREA_RANKING_SERVICE)
    private Service gsmAccessAreaCallFailureRankingService;

    @EJB(beanName = GSM_CFA_TERMINAL_RANKING_SERVICE)
    private Service gsmCallFailureTerminalRankingService;

    @EJB(beanName = GSM_CFA_CONTROLLER_RANKING_SERVICE)
    private Service gsmControllerCallFailureRankingService;

    @EJB(beanName = GSM_CAUSE_CODE_CALLS_DROPPED_FAILURE_RANKING_SERVICE)
    private Service gsmCallFailureCauseCodeCallDropRankingService;

    @EJB(beanName = GSM_SUBSCRIBER_CALL_DROP_RANKING_SERVICE)
    private Service gsmCallFailureSubscriberCallDropRankingService;

    @Path(ACCESS_AREA)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getAccessAreaCallFailureRanking() {
        return gsmAccessAreaCallFailureRankingService.getData(mapResourceLayerParameters());
    }

    @Path(ACCESS_AREA)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getAccessAreaCallFailureRankingAsCSV() {
        return gsmAccessAreaCallFailureRankingService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(TAC)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCallFailureTerminalRanking() {
        return gsmCallFailureTerminalRankingService.getData(mapResourceLayerParameters());
    }

    @Path(TAC)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallFailureTerminalRankingAsCSV() {
        return gsmCallFailureTerminalRankingService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(CALL_FAILURE_RANKING_BY_CALLS_DROPPED_URI)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCauseCodeCallDropCallFailureRanking() {
        return gsmCallFailureCauseCodeCallDropRankingService.getData(mapResourceLayerParameters());
    }

    @Path(CALL_FAILURE_RANKING_BY_CALLS_DROPPED_URI)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCauseCodeCallDropCallFailureRankingAsCSV() {
        return gsmCallFailureCauseCodeCallDropRankingService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(SUBSCRIBER_FOR_CALL_DROP)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getSubscriberCallDropRanking() {
        return gsmCallFailureSubscriberCallDropRankingService.getData(mapResourceLayerParameters());
    }

    @Path(SUBSCRIBER_FOR_CALL_DROP)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getSubscriberCallDropRankingAsCSV() {
        return gsmCallFailureSubscriberCallDropRankingService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(BSC)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getControllerCallFailureRanking() {
        return gsmControllerCallFailureRankingService.getData(mapResourceLayerParameters());
    }

    @Path(BSC)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getControllerCallFailureRankingAsCSV() {
        return gsmControllerCallFailureRankingService.getDataAsCSV(mapResourceLayerParameters(), response);
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
