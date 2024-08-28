/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.gsm.dataconnection;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.resources.gsm.GSMApplicationConstants.*;

import javax.ejb.EJB;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.ericsson.eniq.events.server.common.MediaTypeConstants;
import com.ericsson.eniq.events.server.resources.AbstractResource;
import com.ericsson.eniq.events.server.serviceprovider.Service;

/**
 * @author eatiaro
 * @since 2011
 *
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class GSMDataConnectionDetailedEventAnalysisResource extends AbstractResource {

    private static final String IMSI_GROUP_DETAILED_EVENT_ANALYSIS_SERVICE = "SubscriberGroupDetailedService";

    private static final String IMSI_DETAILED_EVENT_ANALYSIS_BY_SUBCC_SERVICE = "SubscriberSCCDetailedService";

    private static final String IMSI_DETAILED_EVENT_ANALYSIS_BY_CC_SERVICE = "SubscriberCCDetailedService";

    @EJB(beanName = IMSI_GROUP_DETAILED_EVENT_ANALYSIS_SERVICE)
    private Service imsiGroupDetailedEventService;

    @EJB(beanName = IMSI_DETAILED_EVENT_ANALYSIS_BY_SUBCC_SERVICE)
    private Service subscriberDetailedEAbySubCCService;

    @EJB(beanName = IMSI_DETAILED_EVENT_ANALYSIS_BY_CC_SERVICE)
    private Service imsiDetailedEAbyCCService;

    @Path(IMSI_GROUP)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getDataConnectionSubscriberDetailedEventAnalysis() {
        return imsiGroupDetailedEventService.getData(mapResourceLayerParameters());
    }

    @Path(IMSI_GROUP)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getDataConnectionSubscriberDetailedEventAnalysisAsCSV() {
        return imsiGroupDetailedEventService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(IMSI_SCC)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getDataConnectionSubscriberDetailedEAbySubCC() {
        return subscriberDetailedEAbySubCCService.getData(mapResourceLayerParameters());
    }

    @Path(IMSI_SCC)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getDataConnectionSubscriberDetailedEAbySubCCAsCSV() {
        return subscriberDetailedEAbySubCCService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(IMSI_CC)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getDataConnectionImsiDetailedEAbyCC() {
        return imsiDetailedEAbyCCService.getData(mapResourceLayerParameters());
    }

    @Path(IMSI_CC)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getDataConnectionImsiDetailedEAbyCCAsCSV() {
        return imsiDetailedEAbyCCService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.AbstractResource#getService()
     */
    @Override
    protected Service getService() {
        throw new UnsupportedOperationException();
    }
}