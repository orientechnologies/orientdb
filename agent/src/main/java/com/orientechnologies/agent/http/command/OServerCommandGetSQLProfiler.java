/*
 * Copyright 2010-2013 Orient Technologies LTD (info--at--orientechnologies.com)
 * All Rights Reserved. Commercial License.
 * 
 * NOTICE:  All information contained herein is, and remains the property of
 * Orient Technologies LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * Orient Technologies LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * 
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Orient Technologies LTD.
 * 
 * For more information: http://www.orientechnologies.com
 */
package com.orientechnologies.agent.http.command;

import com.orientechnologies.agent.DatabaseProfilerResource;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

import java.io.StringWriter;

public class OServerCommandGetSQLProfiler extends OServerCommandAuthenticatedDbAbstract {
    private static final String[] NAMES = {"GET|sqlProfiler/*"};

    public OServerCommandGetSQLProfiler() {

    }

    @Override
    public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
        final String[] parts = checkSyntax(iRequest.getUrl(), 2, "Syntax error: profiler/<command>/[<config>]|[<from>]");

        iRequest.data.commandInfo = "Profiler information";

        ODatabaseDocumentTx db = getProfiledDatabaseInstance(iRequest);
        OSecurityUser user = db.getUser();

        if (user.checkIfAllowed(ORule.ResourceGeneric.valueOf(DatabaseProfilerResource.PROFILER), null, 2) != null) {


            try {

                final String command = parts[1];
                final String arg = parts.length > 2 ? parts[2] : null;

                final StringWriter jsonBuffer = new StringWriter();
                final OJSONWriter json = new OJSONWriter(jsonBuffer);
                json.append(Orient.instance().getProfiler().toJSON("realtime", "db." + db.getName() + ".command"));

                iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, jsonBuffer.toString(), null);

            } catch (Exception e) {
                iResponse.send(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, e, null);
            }
        } else {
            iResponse.send(OHttpUtils.STATUS_AUTH_CODE, OHttpUtils.STATUS_AUTH_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, null, null);
        }
        return false;
    }

    @Override
    public String[] getNames() {
        return NAMES;
    }
}
