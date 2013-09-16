/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.monitor.http;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.*;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAbstract;

public class OServerCommandGetLoggedUserInfo extends OServerCommandAbstract {
    private static final String[] NAMES = {"GET|loggedUserInfo/*"};


    public OServerCommandGetLoggedUserInfo(final OServerCommandConfiguration iConfiguration) {
    }

    @Override
    public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
        OHttpSession session = OHttpSessionManager.getInstance().getSession(iRequest.sessionId);
        try {
            ODocument document = new ODocument();
                              document.field("user", session.getUserName());
                              document.field("database", session.getDatabaseName());
                              document.field("host", session.getParameter("host"));
                              document.field("port", session.getParameter("port"));
            iResponse.writeResult(document, "indent:6");
        } catch (Exception e) {
            iResponse.send(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, e, null);
        }
        return false;
    }


    @Override
    public String[] getNames() {
        return NAMES;
    }
}
