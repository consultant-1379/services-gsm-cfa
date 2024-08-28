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
 * @author ejoegaf
 *
 */
@Stateless
//@TransactionManagement(TransactionManagementType.BEAN)
@LocalBean
public class GSMCallFailureCauseCodeAnalysisResource extends AbstractResource {

    private static final String GSM_CFA_CONTROLLER_CAUSE_CODE_ANALYSIS_SERVICE = "ControllerCCService";

    private static final String GSM_CFA_CONTROLLER_SUB_CAUSE_CODE_ANALYSIS_SERVICE = "ControllerSubCCService";

    private static final String GSM_CFA_TERMINAL_SUB_CAUSE_CODE_ANALYSIS_SERVICE = "TerminalSubCCService";

    private static final String GSM_CALL_FAILURE_ACCESS_AREA_CAUSE_CODE_LIST_SERVICE = "AccessAreaCCListService";

    private static final String GSM_CALL_FAILURE_CONTROLLER_CAUSE_CODE_LIST_SERVICE = "ControllerCCListService";

    private static final String CAUSE_CODE_TABLE = "GSMControllerCauseCodeTableService";

    private static final String SUB_CAUSE_CODE_TABLE = "GSMControllerSubCauseCodeTableService";

    private static final String GSM_CALL_FAILURE_ACCESS_AREA_CAUSE_CODE_SERVICE = "AccessAreaCCService";

    private static final String GSM_CALL_FAILURE_ACCESS_AREA_SUB_CAUSE_CODE_SERVICE = "AccessAreaSubCCService";

    private static final String GSM_CALL_FAILURE_ACCESS_AREA_SUB_CAUSE_CODE_DETAIL_SERVICE = "AccessAreaSubCCDetailedService";

    private static final String GSM_CALL_FAILURE_CONTROLLER_SUB_CAUSE_CODE_DETAILED_EVENT_ANALYSIS_SERVICE = "ControllerSubCCDetailedService";

    private static final String GSM_CALL_FAILURE_CONTROLLER_SUB_CAUSE_CODE_DETAIL_SERVICE = "ControllerSubCCDetailedAnalysisService";

    private static final String GSM_CONTROLLER_GROUP_SUMMARY_BREAKDOWN_SERVICE = "ControllerGroupBreakdownService";

    private static final String GSM_CFA_TERMINAL_CAUSE_GROUP_SUMMARY_SERVICE = "TerminalCCService";

    private static final String GSM_ACCESS_AREA_GROUP_SUMMARY_BREAKDOWN_SERVICE = "AccessAreaGroupBreakdownService";

    private static final String GSM_CFA_ACCESS_AREA_DISTRIBUTION_SUMMARY_SERVICE = "AccessAreaDistributionCCSummaryService";

    @EJB(beanName = GSM_CFA_CONTROLLER_CAUSE_CODE_ANALYSIS_SERVICE)
    private Service gsmCallFailureControllerCauseCodeService;

    @EJB(beanName = GSM_CFA_CONTROLLER_SUB_CAUSE_CODE_ANALYSIS_SERVICE)
    private Service gsmCallFailureControllerSubCauseCodeService;

    @EJB(beanName = GSM_CFA_TERMINAL_SUB_CAUSE_CODE_ANALYSIS_SERVICE)
    private Service gsmCallFailureTerminalSubCauseCodeService;

    @EJB(beanName = GSM_CALL_FAILURE_ACCESS_AREA_CAUSE_CODE_LIST_SERVICE)
    private Service gsmCallFailureAccessAreaCauseCodeListService;

    @EJB(beanName = GSM_CALL_FAILURE_CONTROLLER_CAUSE_CODE_LIST_SERVICE)
    private Service gsmCallFailureControllerCauseCodeListService;

    @EJB(beanName = GSM_CALL_FAILURE_ACCESS_AREA_CAUSE_CODE_SERVICE)
    private Service gsmCallFailureAccessAreaCauseCodeAnalysisService;

    @EJB(beanName = GSM_CALL_FAILURE_ACCESS_AREA_SUB_CAUSE_CODE_SERVICE)
    private Service gsmCallFailureAccessAreaSubCauseCodeAnalysisService;

    @EJB(beanName = GSM_CALL_FAILURE_ACCESS_AREA_SUB_CAUSE_CODE_DETAIL_SERVICE)
    private Service gsmCallFailureAccessAreaSCCDetailedAnalysisService;

    @EJB(beanName = GSM_CALL_FAILURE_CONTROLLER_SUB_CAUSE_CODE_DETAILED_EVENT_ANALYSIS_SERVICE)
    private Service gsmCallFailureControllerSubCauseCodeDetailedEventAnalysisService;

    @EJB(beanName = GSM_CALL_FAILURE_CONTROLLER_SUB_CAUSE_CODE_DETAIL_SERVICE)
    private Service gsmCallFailureControllerSubCauseCodeDetailedAnalysisService;

    @EJB(beanName = GSM_CONTROLLER_GROUP_SUMMARY_BREAKDOWN_SERVICE)
    private Service gsmControllerGroupBreakdownService;

    @EJB(beanName = GSM_CFA_TERMINAL_CAUSE_GROUP_SUMMARY_SERVICE)
    private Service gsmTerminalCCService;

    @EJB(beanName = CAUSE_CODE_TABLE)
    private Service gsmCallFailureCauseCodeTableService;

    @EJB(beanName = SUB_CAUSE_CODE_TABLE)
    private Service gsmCallFailureSubCauseCodeTableService;

    @EJB(beanName = GSM_ACCESS_AREA_GROUP_SUMMARY_BREAKDOWN_SERVICE)
    private Service gsmAccessAreaGroupBreakdownService;

    @EJB(beanName = GSM_CFA_ACCESS_AREA_DISTRIBUTION_SUMMARY_SERVICE)
    private Service gsmAccessAreaDistributionSummaryService;

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.AbstractResource#getService()
     */
    @Override
    protected Service getService() {
        throw new UnsupportedOperationException();
    }

    @Path(CC_LIST)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCallFailureCauseCodeList() {
        final MultivaluedMap<String, String> reqParams = mapResourceLayerParameters();
        final String type = reqParams.getFirst(TYPE_PARAM);
        if (TYPE_BSC.equals(type)) {
            return gsmCallFailureControllerCauseCodeListService.getData(reqParams);
        }
        return gsmCallFailureAccessAreaCauseCodeListService.getData(reqParams);
    }

    @Path(CC_LIST)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallFailureCauseCodeListAsCSV() {
        throw new UnsupportedOperationException();
    }

    @Path(CAUSE_CODE_PIE_CHART)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCallFailureCauseCodePieChart() {
        final MultivaluedMap<String, String> reqParams = mapResourceLayerParameters();
        final String type = reqParams.getFirst(TYPE_PARAM);
        if (TYPE_BSC.equals(type)) {
            return gsmCallFailureControllerCauseCodeService.getData(reqParams);
        }
        return gsmCallFailureAccessAreaCauseCodeAnalysisService.getData(reqParams);
    }

    @Path(CAUSE_CODE_PIE_CHART)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallFailureCauseCodePieChartAsCSV() {
        throw new UnsupportedOperationException();
    }

    @Path(CAUSE_CODE_GRID)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCallFailureCauseCodeAnalysisGrid() {
        final MultivaluedMap<String, String> reqParams = mapResourceLayerParameters();
        final String type = reqParams.getFirst(TYPE_PARAM);
        if (TYPE_BSC.equals(type)) {
            return gsmCallFailureControllerCauseCodeService.getData(reqParams);
        }
        if (TYPE_TAC.equals(type)) {
            return gsmTerminalCCService.getData(reqParams);
        }
        return gsmCallFailureAccessAreaCauseCodeAnalysisService.getData(reqParams);
    }

    @Path(CAUSE_CODE_GRID)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallFailureCauseCodeAnalysisGridAsCSV() {
        final MultivaluedMap<String, String> reqParams = mapResourceLayerParameters();
        final String type = reqParams.getFirst(TYPE_PARAM);
        if (TYPE_BSC.equals(type)) {
            return gsmCallFailureControllerCauseCodeService.getDataAsCSV(reqParams, response);
        }
        if (TYPE_TAC.equals(type)) {
            return gsmTerminalCCService.getDataAsCSV(reqParams, response);
        }
        return gsmCallFailureAccessAreaCauseCodeAnalysisService.getDataAsCSV(reqParams, response);
    }

    @Path(NETWORK_CAUSE_CODE_GRID)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCallFailureNetworkCauseCodeAnalysisGrid() {
        final MultivaluedMap<String, String> reqParams = mapResourceLayerParameters();
        final String type = reqParams.getFirst(TYPE_PARAM);
        if (TYPE_BSC.equals(type)) {
            return gsmCallFailureControllerCauseCodeService.getData(reqParams);
        }
        if (TYPE_TAC.equals(type)) {
            return gsmTerminalCCService.getData(reqParams);
        }
        return gsmCallFailureAccessAreaCauseCodeAnalysisService.getData(reqParams);
    }

    @Path(NETWORK_CAUSE_CODE_GRID)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallFailureNetworkCauseCodeAnalysisGridAsCSV() {
        final MultivaluedMap<String, String> reqParams = mapResourceLayerParameters();
        final String type = reqParams.getFirst(TYPE_PARAM);
        if (TYPE_BSC.equals(type)) {
            return gsmCallFailureControllerCauseCodeService.getDataAsCSV(reqParams, response);
        }
        if (TYPE_TAC.equals(type)) {
            return gsmTerminalCCService.getDataAsCSV(reqParams, response);
        }
        return gsmCallFailureAccessAreaCauseCodeAnalysisService.getDataAsCSV(reqParams, response);
    }

    @Path(SUB_CAUSE_CODE_GRID)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCallFailureSubCauseCodeAnalysisGrid() {
        final MultivaluedMap<String, String> reqParams = mapResourceLayerParameters();
        final String type = reqParams.getFirst(TYPE_PARAM);
        if (TYPE_BSC.equals(type)) {
            return gsmCallFailureControllerSubCauseCodeService.getData(mapResourceLayerParameters());

        }

        if (TYPE_TAC.equals(type)) {
            return gsmCallFailureTerminalSubCauseCodeService.getData(mapResourceLayerParameters());

        }
        return gsmCallFailureAccessAreaSubCauseCodeAnalysisService.getData(mapResourceLayerParameters());
    }

    @Path(SUB_CAUSE_CODE_GRID)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallFailureSubCauseCodeAnalysisGridAsCSV() {
        final MultivaluedMap<String, String> reqParams = mapResourceLayerParameters();
        final String type = reqParams.getFirst(TYPE_PARAM);
        if (TYPE_BSC.equals(type)) {
            return gsmCallFailureControllerSubCauseCodeService.getDataAsCSV(mapResourceLayerParameters(), response);
        }

        if (TYPE_TAC.equals(type)) {
            return gsmCallFailureTerminalSubCauseCodeService.getDataAsCSV(mapResourceLayerParameters(), response);
        }
        return gsmCallFailureAccessAreaSubCauseCodeAnalysisService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(NETWORK_SUB_CAUSE_CODE_GRID)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getNetworkCallFailureSubCauseCodeAnalysisGrid() {
        final MultivaluedMap<String, String> reqParams = mapResourceLayerParameters();
        final String type = reqParams.getFirst(TYPE_PARAM);
        if (TYPE_BSC.equals(type)) {
            return gsmCallFailureControllerSubCauseCodeService.getData(mapResourceLayerParameters());

        }

        if (TYPE_TAC.equals(type)) {
            return gsmCallFailureTerminalSubCauseCodeService.getData(mapResourceLayerParameters());

        }
        return gsmCallFailureAccessAreaSubCauseCodeAnalysisService.getData(mapResourceLayerParameters());
    }

    @Path(NETWORK_SUB_CAUSE_CODE_GRID)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getNetworkCallFailureSubCauseCodeAnalysisGridAsCSV() {
        final MultivaluedMap<String, String> reqParams = mapResourceLayerParameters();
        final String type = reqParams.getFirst(TYPE_PARAM);
        if (TYPE_BSC.equals(type)) {
            return gsmCallFailureControllerSubCauseCodeService.getDataAsCSV(mapResourceLayerParameters(), response);
        }

        if (TYPE_TAC.equals(type)) {
            return gsmCallFailureTerminalSubCauseCodeService.getDataAsCSV(mapResourceLayerParameters(), response);
        }
        return gsmCallFailureAccessAreaSubCauseCodeAnalysisService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(SUB_CAUSE_CODE_PIE_CHART)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCallFailureSubCauseCodePieChart() {
        final MultivaluedMap<String, String> reqParams = mapResourceLayerParameters();
        final String type = reqParams.getFirst(TYPE_PARAM);
        if (TYPE_BSC.equals(type)) {
            return gsmCallFailureControllerSubCauseCodeService.getData(reqParams);
        }
        return gsmCallFailureAccessAreaSubCauseCodeAnalysisService.getData(reqParams);
    }

    @Path(SUB_CAUSE_CODE_PIE_CHART)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallFailureSubCauseCodePieChartAsCSV() {
        throw new UnsupportedOperationException();
    }

    @Path(BSC_GROUP_BREAKDOWN)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCallFailureEventSummaryControllerGroupBreakDown() {
        return gsmControllerGroupBreakdownService.getData(mapResourceLayerParameters());
    }

    @Path(BSC_GROUP_BREAKDOWN)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallFailureEventSummaryControllerGroupBreakDownCSV() {
        return gsmControllerGroupBreakdownService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(DETAIL_SUB_CAUSE_CODE_GRID)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCallFailureSubCauseCodeDetailedGrid() {
        final MultivaluedMap<String, String> reqParams = mapResourceLayerParameters();
        final String type = reqParams.getFirst(TYPE_PARAM);
        if (TYPE_BSC.equals(type)) {
            return gsmCallFailureControllerSubCauseCodeDetailedEventAnalysisService.getData(reqParams);
        }
        return gsmCallFailureAccessAreaSCCDetailedAnalysisService.getData(reqParams);
    }

    @Path(DETAIL_SUB_CAUSE_CODE_GRID)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallFailureSubCauseCodeDetailedGridAsCSV() {
        final MultivaluedMap<String, String> reqParams = mapResourceLayerParameters();
        final String type = reqParams.getFirst(TYPE_PARAM);
        if (TYPE_BSC.equals(type)) {
            return gsmCallFailureControllerSubCauseCodeDetailedEventAnalysisService.getDataAsCSV(
                    mapResourceLayerParameters(), response);
        }
        return gsmCallFailureAccessAreaSCCDetailedAnalysisService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(DETAIL_CC_SUB_CAUSE_CODE_GRID)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCallFailureCCSubCauseCodeDetailedGrid() {
        final MultivaluedMap<String, String> reqParams = mapResourceLayerParameters();
        final String type = reqParams.getFirst(TYPE_PARAM);
        if (TYPE_BSC.equals(type)) {
            return gsmCallFailureControllerSubCauseCodeDetailedAnalysisService.getData(reqParams);
        }
        return gsmCallFailureAccessAreaSCCDetailedAnalysisService.getData(reqParams);
    }

    @Path(DETAIL_CC_SUB_CAUSE_CODE_GRID)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallFailureCCSubCauseCodeDetailedGridAsCSV() {
        final MultivaluedMap<String, String> reqParams = mapResourceLayerParameters();
        final String type = reqParams.getFirst(TYPE_PARAM);
        if (TYPE_BSC.equals(type)) {
            return gsmCallFailureControllerSubCauseCodeDetailedAnalysisService.getDataAsCSV(
                    mapResourceLayerParameters(), response);
        }
        return gsmCallFailureAccessAreaSCCDetailedAnalysisService.getDataAsCSV(mapResourceLayerParameters(), response);
    }

    @Path(CAUSE_CODE_TABLE_CC_GSM)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCauseCodeTable() {
        final MultivaluedMap<String, String> reqParams = mapResourceLayerParameters();
        final String type = reqParams.getFirst(TYPE_PARAM);
        if (TYPE_BSC.equals(type)) {
            return gsmCallFailureCauseCodeTableService.getData(reqParams);
        }
        return gsmCallFailureCauseCodeTableService.getData(reqParams);
    }

    @Path(CAUSE_CODE_TABLE_CC_GSM)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCauseCodeTableAsCSV() {
        throw new UnsupportedOperationException();
    }

    @Path(CAUSE_CODE_TABLE_SCC_GSM)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getSubCauseCodeTable() {
        final MultivaluedMap<String, String> reqParams = mapResourceLayerParameters();
        return gsmCallFailureSubCauseCodeTableService.getData(reqParams);
    }

    @Path(CAUSE_CODE_TABLE_SCC_GSM)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getSubCauseCodeTableAsCSV() {
        throw new UnsupportedOperationException();
    }

    @Path(ACCESS_AREA_GROUP_BREAKDOWN)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getCallFailureEventSummaryAccessAreaGroupBreakDown() {
        return gsmAccessAreaGroupBreakdownService.getData(mapResourceLayerParameters());
    }

    @Path(ACCESS_AREA_GROUP_BREAKDOWN)
    @GET
    @Produces(MediaTypeConstants.APPLICATION_CSV)
    public Response getCallFailureEventSummaryAccessAreaGroupBreakDownCSV() {
        return gsmAccessAreaGroupBreakdownService.getDataAsCSV(mapResourceLayerParameters(), response);
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
     * @see
     * com.ericsson.eniq.events.server.resources.AbstractResource#getDataAsCSV()
     */
    @Override
    public Response getDataAsCSV() throws WebApplicationException {
        throw new UnsupportedOperationException();
    }

}
