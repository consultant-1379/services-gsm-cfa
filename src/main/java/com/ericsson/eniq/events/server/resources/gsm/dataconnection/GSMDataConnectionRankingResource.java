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
public class GSMDataConnectionRankingResource extends AbstractResource {

    private static final String SUBSCRIBER_DATA_VOL_RANKING_SERVICE = "SubscriberDataVolumeRankingService";

    private static final String CONTROLLER_FAILURE_RANKING_SERVICE = "ControllerFailureRankingService";

    private static final String ACCESS_AREA_DATA_VOL_RANKING_SERVICE = "AccessAreaDataVolumeRankingService";

    private static final String CONTROLLER_DATA_VOL_RANKING_SERVICE = "ControllerDataVolumeRankingService";

    private static final String IMSI_FAILURE_RANKING_SERVICE = "SubscriberFailureRankingService";

    @EJB(beanName = SUBSCRIBER_DATA_VOL_RANKING_SERVICE)
    private Service subscriberDataVolumeRankingService;

    @EJB(beanName = ACCESS_AREA_DATA_VOL_RANKING_SERVICE)
    private Service accessAreaDataVolumeRankingService;

    @EJB(beanName = CONTROLLER_DATA_VOL_RANKING_SERVICE)
    private Service ControllerDataVolumeRankingService;

    @Path(SUBSCRIBER_DATAVOL)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getSubscriberDataVolumeRanking() {
        return subscriberDataVolumeRankingService.getData(mapResourceLayerParameters());
    }

    @Path(SUBSCRIBER_DATAVOL)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getSubscriberDataVolumeRankingAsCSV() {
        return subscriberDataVolumeRankingService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(ACCESS_AREA_DATAVOL)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getAccessAreaDataVolumeRanking() {
        return accessAreaDataVolumeRankingService.getData(mapResourceLayerParameters());
    }

    @Path(ACCESS_AREA_DATAVOL)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getAccessAreaDataVolumeRankingAsCSV() {
        return accessAreaDataVolumeRankingService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(CONTROLLER_DATAVOL)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getControllerDataVolumeRanking() {
        return ControllerDataVolumeRankingService.getData(mapResourceLayerParameters());
    }

    @Path(CONTROLLER_DATAVOL)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getControllerDataVolumeRankingAsCSV() {
        return ControllerDataVolumeRankingService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Override
    protected Service getService() {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ericsson.eniq.events.server.resources.AbstractResource#getData()
     */
    @Override
    public String getData() throws WebApplicationException {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.ericsson.eniq.events.server.resources.AbstractResource#getDataAsCSV()
     */
    @Override
    public Response getDataAsCSV() throws WebApplicationException {
        throw new UnsupportedOperationException();
    }

    @EJB(beanName = CONTROLLER_FAILURE_RANKING_SERVICE)
    private Service controllerFailureRankingService;

    @Path(CONTROLLER_FAILURE)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getFailureRankingByControllerData() {
        return controllerFailureRankingService.getData(mapResourceLayerParameters());
    }

    @Path(CONTROLLER_FAILURE)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getFailureRankingByControllerDataAsCSV() {
        return controllerFailureRankingService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @EJB(beanName = IMSI_FAILURE_RANKING_SERVICE)
    private Service imsiFailureRankingService;

    @Path(IMSI_FAILURE)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getFailureRankingByImsiData() {
        return imsiFailureRankingService.getData(mapResourceLayerParameters());
    }

    @Path(IMSI_FAILURE)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getFailureRankingByImsiDataAsCSV() {
        return imsiFailureRankingService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

}
