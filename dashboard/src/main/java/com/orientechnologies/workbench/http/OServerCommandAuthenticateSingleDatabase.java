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
package com.orientechnologies.workbench.http;

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.network.protocol.http.*;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import com.orientechnologies.workbench.OMonitoredServer;
import com.orientechnologies.workbench.OWorkbenchPlugin;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class OServerCommandAuthenticateSingleDatabase extends OServerCommandAuthenticatedDbAbstract {
  private static final String[] NAMES = { "GET|authenticateUser/*" };
  private static String         host  = "localhost";
  private static String         port  = "2424";
  private static String         user;

  public OServerCommandAuthenticateSingleDatabase() {

  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    ODocument result = new ODocument();
    result.field("authenticated", "true");
    result.field("user", user);
    result.field("host", host);
    result.field("port", port);
    iResponse.writeResult(result, null, null);

    return false;
  }

  @Override
  protected boolean authenticate(final OHttpRequest iRequest, final OHttpResponse iResponse,
      final List<String> iAuthenticationParts, final String iDatabaseName) throws IOException {
    try {

      final String[] urlParts = iRequest.url.substring(1).split("/");

      OWorkbenchPlugin monitorPlugin = OServerMain.server().getPluginByClass(OWorkbenchPlugin.class);

      ODatabaseDocumentTx db = null;
      try {
        if (urlParts.length == 2) {
          db = (ODatabaseDocumentTx) OServerMain.server().openDatabase("graph", iDatabaseName, iAuthenticationParts.get(0),
              iAuthenticationParts.get(1));
        } else {
          boolean authorizedServer = false;
          String host = urlParts[2];
          String port = (urlParts.length == 3 && !urlParts[3].equals("") ? "2424" : urlParts[3]);
          for (Map.Entry o : monitorPlugin.getMonitoredServers()) {
            if (((OMonitoredServer) o.getValue()).getConfiguration().field("url").toString().contains(host + ":" + port)) {
              authorizedServer = true;
            }
          }
          if (authorizedServer) {
            db = new ODatabaseDocumentTx("remote:" + host + ":" + port + "/" + iDatabaseName).open(iAuthenticationParts.get(0),
                iAuthenticationParts.get(1));
          } else {
            throw new OSecurityAccessException(host + ":" + port, "Server not under monitoring, access denied");
          }
        }

        // AUTHENTICATED: CREATE THE SESSION
        iRequest.sessionId = OHttpSessionManager.getInstance().createSession(iDatabaseName, iAuthenticationParts.get(0),
            iAuthenticationParts.get(1));
        iResponse.sessionId = iRequest.sessionId;
        OHttpSession session = OHttpSessionManager.getInstance().getSession(iRequest.sessionId);
        if (host != null) {
          session.setParameter("host", host);
          session.setParameter("port", port);
        }
        user = iAuthenticationParts.get(0);
        return true;

      } catch (OSecurityAccessException e) {
        // WRONG USER/PASSWD
        OLogManager.instance().error(this,
            "Cannot login to the database '" + iDatabaseName + "', the server is not under monitoring",
            OSecurityAccessException.class, e);
      } catch (OLockException e) {
        OLogManager.instance().error(this, "Cannot access to the database '" + iDatabaseName + "'", ODatabaseException.class, e);
      } finally {
        if (db != null)
          db.close();
        else
          // WRONG USER/PASSWD
          sendAuthorizationRequest(iRequest, iResponse, iDatabaseName);
      }
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
