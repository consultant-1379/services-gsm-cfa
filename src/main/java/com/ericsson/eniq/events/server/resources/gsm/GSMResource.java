/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.gsm;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.resources.gsm.GSMApplicationConstants.*;

import javax.ejb.EJB;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ws.rs.Path;

import com.ericsson.eniq.events.server.resources.gsm.callfailure.GSMCallFailureCauseCodeAnalysisResource;
import com.ericsson.eniq.events.server.resources.gsm.callfailure.GSMCallFailureDetailedEventAnalysisResource;
import com.ericsson.eniq.events.server.resources.gsm.callfailure.GSMCallFailureEventSummaryResource;
import com.ericsson.eniq.events.server.resources.gsm.callfailure.GSMCallFailureRankingResource;
import com.ericsson.eniq.events.server.resources.gsm.callfailure.GSMRoamingResource;
import com.ericsson.eniq.events.server.resources.gsm.dataconnection.GSMDataConnectionCauseCodeResource;
import com.ericsson.eniq.events.server.resources.gsm.dataconnection.GSMDataConnectionDataVolumeAnalysisResource;
import com.ericsson.eniq.events.server.resources.gsm.dataconnection.GSMDataConnectionDetailedEventAnalysisResource;
import com.ericsson.eniq.events.server.resources.gsm.dataconnection.GSMDataConnectionEventSummaryResource;
import com.ericsson.eniq.events.server.resources.gsm.dataconnection.GSMDataConnectionRankingResource;

/**
 * The Class GSMResource. Sub-root resource of RESTful service.
 * 
 * @author ejoegaf
 * @since Sept 2011
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class GSMResource {

    @EJB
    protected GSMCallFailureRankingResource gsmCallFailureRankingResource;

    @EJB
    protected GSMCallFailureEventSummaryResource gsmCallFailureEventSummaryResource;

    @EJB
    protected GSMCallFailureDetailedEventAnalysisResource gsmCallFailureDetailedEventAnalysisResource;

    @EJB
    protected GSMCallFailureCauseCodeAnalysisResource gsmCallFailureCauseCodeAnalysisResource;

    @EJB
    protected GSMDataConnectionRankingResource gsmDataConnectionRankingResource;

    @EJB
    protected GSMDataConnectionDetailedEventAnalysisResource detailedEventAnalysisResource;

    @EJB
    protected GSMDataConnectionEventSummaryResource dataConnectionEventSummaryResource;

    @EJB
    protected GSMDataConnectionDataVolumeAnalysisResource gsmDataConnectionDataVolumeAnalysisResource;

    @EJB
    protected GSMDataConnectionCauseCodeResource gsmDataConnectionCauseCodeAnalysisResource;

    @EJB
    protected GSMRoamingResource gsmCallFailureRomaingResource;

    @Path(CALL_FAILURE_RANKING_ANALYSIS)
    public GSMCallFailureRankingResource getCallFailureRankingResource() {
        return this.gsmCallFailureRankingResource;
    }

    @Path(CALL_FAILURE_EVENT_SUMMARY)
    public GSMCallFailureEventSummaryResource getCallFailureEventSummaryResource() {
        return this.gsmCallFailureEventSummaryResource;
    }

    @Path(CALL_FAILURE_DETAILED_EVENT_ANALYSIS)
    public GSMCallFailureDetailedEventAnalysisResource getCallFailureDetailedEventAnalysisResource() {
        return this.gsmCallFailureDetailedEventAnalysisResource;
    }

    @Path(CALL_FAILURE_CAUSE_CODE_ANALYSIS)
    public GSMCallFailureCauseCodeAnalysisResource getCallFailureCauseCodeAnalysisResource() {
        return this.gsmCallFailureCauseCodeAnalysisResource;
    }

    @Path(DATA_CONNECTION_RANKING_ANALYSIS)
    public GSMDataConnectionRankingResource getDataConnectionRankingResource() {
        return this.gsmDataConnectionRankingResource;
    }

    @Path(DATA_CONNECTION_DETAILED_EVENT_ANALYSIS)
    public GSMDataConnectionDetailedEventAnalysisResource getDataConnectionDetailedEventAnalysisResource() {
        return this.detailedEventAnalysisResource;
    }

    @Path(DATA_CONNECTION_EVENT_SUMMARY)
    public GSMDataConnectionEventSummaryResource getDataConnectionEventSummaryResource() {
        return this.dataConnectionEventSummaryResource;
    }

    @Path(DATA_CONNECTION_DATAVOLUME_ANALYSIS)
    public GSMDataConnectionDataVolumeAnalysisResource getDataConnectionDataVolumeAnalysisResource() {
        return this.gsmDataConnectionDataVolumeAnalysisResource;
    }

    @Path(DATA_CONNECTION_CAUSE_CODE_ANALYSIS)
    public GSMDataConnectionCauseCodeResource getDataConnectionCauseCodeAnalysisResource() {
        return this.gsmDataConnectionCauseCodeAnalysisResource;
    }

    @Path(CALL_FAILURE_ROAMING_ANALYSIS)
    public GSMRoamingResource getCallFailureRoamingAnalysisResource() {
        return this.gsmCallFailureRomaingResource;
    }
}
