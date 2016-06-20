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
package com.orientechnologies.agent.http.command;

import com.orientechnologies.agent.proxy.HttpProxyListener;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.util.Date;

public class OServerCommandPostBackupDatabase extends OServerCommandDistributedScope implements OCommandOutputListener {
  public OServerCommandPostBackupDatabase() {
    super("database.backup");
  }

  private static final String[] NAMES = { "GET|backup/*", "POST|backup/*" };

  @Override
  public boolean execute(final OHttpRequest iRequest, final OHttpResponse iResponse) throws Exception {
    final String[] urlParts = checkSyntax(iRequest.url, 2, "Syntax error: backup/<database>");

    iRequest.data.commandInfo = "Database backup";
    try {
      iRequest.databaseName = urlParts[1];

      if (isLocalNode(iRequest)) {

        final ODatabaseDocument database = getProfiledDatabaseInstance(iRequest);

        try {
          iResponse.writeStatus(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION);
          iResponse.writeHeaders(OHttpUtils.CONTENT_GZIP);
          iResponse.writeLine("Content-Disposition: attachment; filename=" + database.getName() + ".gz");
          iResponse.writeLine("Date: " + new Date());
          iResponse.writeLine(null);

          // TODO
          database.backup(iResponse.getOutputStream(), null, null, null, 0, 0);

          try {
            iResponse.flush();
          } catch (SocketException e) {
          }
        } finally {
          if (database != null)
            database.close();
        }
      } else {
        proxyRequest(iRequest, iResponse, new HttpProxyListener() {
          @Override
          public void onProxySuccess(OHttpRequest request, OHttpResponse response, InputStream is) throws IOException {
            iResponse.sendStream(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, OHttpUtils.CONTENT_GZIP, is, -1,
                urlParts[1] + ".gz");
          }

          @Override
          public void onProxyError(OHttpRequest request, OHttpResponse response, InputStream is, int code, Exception e)
              throws IOException {
            iResponse.send(code, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_JSON, e, null);
          }
        });
      }
    } catch (Exception e) {
      iResponse.sendStream(404, "File not found", null, null, 0);
    }
    return false;
  }



  @Override
  public void onMessage(String iText) {
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
