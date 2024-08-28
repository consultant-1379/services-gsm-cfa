/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.serviceprovider.impl.gsm.callfailure.cc;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import javax.ejb.Local;
import javax.ejb.Stateless;

import com.ericsson.eniq.events.server.serviceprovider.Service;
import com.ericsson.eniq.events.server.serviceprovider.impl.GenericSimpleService;

/**
 * @author eprjaya
 * @since 2011
 *
 */
@Stateless
@Local(Service.class)
public class GSMControllerCauseCodeTableService extends GenericSimpleService {

    @Override
    public String getTemplatePath() {
        return GSM_CFA_CAUSE_CODE_TABLE;
    }

}
