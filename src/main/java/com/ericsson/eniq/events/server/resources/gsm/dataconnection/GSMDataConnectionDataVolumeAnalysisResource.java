/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.gsm.dataconnection;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

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
 * @author ETHOMIT
 * @since 2012
 * 
 */

@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class GSMDataConnectionDataVolumeAnalysisResource extends AbstractResource {

    private static final String SUBSCRIBER_DATAVOLUME_ANALYSIS_SERVICE = "SubscriberDataVolumeService";

    @EJB(beanName = SUBSCRIBER_DATAVOLUME_ANALYSIS_SERVICE)
    private Service subscriberDataVolumeAnalysisService;

    @Path(SUBSCRIBER)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getDataConnectionSubscriberDataVolumeAnalysis() {
        return subscriberDataVolumeAnalysisService.getData(mapResourceLayerParameters());
    }

    @Path(SUBSCRIBER)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getDataConnectionSubscriberDataVolumeAnalysisAsCSV() {
        return subscriberDataVolumeAnalysisService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.ericsson.eniq.events.server.resources.AbstractResource#getService()
     */
    @Override
    protected Service getService() {
        throw new UnsupportedOperationException();
    }

}
