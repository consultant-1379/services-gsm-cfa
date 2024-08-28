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
 * @since 2012
 *
 */
@Stateless
@LocalBean
public class GSMDataConnectionEventSummaryResource extends AbstractResource {

    private static final String IMSI_GROUP_FAILURE_SUMMARY_SERVICE = "SubscriberGroupSummaryService";

    private static final String SUBSCRIBER_DETAILED_EVENT_ANALYSIS_SERVICE = "DataConnectionSubscriberDetailedService";

    @EJB(beanName = SUBSCRIBER_DETAILED_EVENT_ANALYSIS_SERVICE)
    private Service subscriberDetailedEventAnalysisService;

    @EJB(beanName = IMSI_GROUP_FAILURE_SUMMARY_SERVICE)
    private Service imsiGroupFailureSummaryService;

    @Path(IMSI_GROUP)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getImsiGroupSummaryDataJSON() {
        return imsiGroupFailureSummaryService.getData(mapResourceLayerParameters());
    }

    @Path(IMSI_GROUP)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getImsiGroupSummaryDataCSV() {
        return imsiGroupFailureSummaryService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    //@Warning : this here is actually a path to a detailed event analysis.
    // This was necessary because there is no Summary view for IMSI thus
    // Connection Failure button under the Subscriber TAB in the UI needs to go 
    // to two separate type of windows. For this to be possible this compromise had to be implemented.
    @Path(IMSI)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getImsiDetailedEventDataJSON() {
        return subscriberDetailedEventAnalysisService.getData(mapResourceLayerParameters());
    }

    @Path(IMSI)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getImsiDetailedEventDataCSV() {
        return subscriberDetailedEventAnalysisService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.AbstractResource#getService()
     */
    @Override
    protected Service getService() {
        throw new UnsupportedOperationException();
    }
}