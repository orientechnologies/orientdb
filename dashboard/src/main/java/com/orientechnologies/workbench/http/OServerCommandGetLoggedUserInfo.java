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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpSession;
import com.orientechnologies.orient.server.network.protocol.http.OHttpSessionManager;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public class OServerCommandGetLoggedUserInfo extends OServerCommandAuthenticatedDbAbstract {
  private static final String[] NAMES = { "GET|loggedUserInfo/*", "POST|loggedUserInfo/*" };

  public OServerCommandGetLoggedUserInfo() {
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    OHttpSession session = OHttpSessionManager.getInstance().getSession(iRequest.sessionId);
    final String[] urlParts = checkSyntax(iRequest.url, 1, "Syntax error: loggedUserInfo/<db>/<type>");

    String command = urlParts[2];

    ODatabaseDocumentTx db = getProfiledDatabaseInstance(iRequest);
    ODatabaseRecordThreadLocal.INSTANCE.set(db);
    if ("configuration".equals(command)) {
      if (iRequest.httpMethod.equals("GET")) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("user", session.getUserName());
        final List<OIdentifiable> response = db.query(new OSQLSynchQuery<ORecordSchemaAware<?>>(
            "select from UserConfiguration where user.name = :user"), params);
        iResponse.writeRecords(response, "*:1");
      } else {

        ODocument doc = new ODocument().fromJSON(iRequest.content);
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("name", session.getUserName());
        List<ODocument> users = db.query(new OSQLSynchQuery<ORecordSchemaAware<?>>("select from OUser where name = :name"), params);
        ODocument user = users.iterator().next();
        doc.field("user", user);
        doc.save();
        iResponse.writeResult(doc, "*:1", null);
      }
      return false;

    } else if ("version".equals(command)) {

      return false;
    } else if ("changePassword".equals(command)) {
      if (iRequest.httpMethod.equals("POST")) {
        ODocument doc = new ODocument().fromJSON(iRequest.content);

        String oldpassword = doc.field("oldpassword");
        if (oldpassword != null) {

          // TODO CHECK THE OLD PASSWORD WITH DB
          if (oldpassword.equals(session.getUserPassword())) {
            Object reset = null;
            doc.field("oldpassword", reset);
            db.save(doc);
            iResponse.send(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, null, null);
          } else {
            throw new RuntimeException("Wrong old password.");
          }
        }
        throw new RuntimeException("Old password missing.");
      }
      return false;
    } else {
      try {
        ODocument document = new ODocument();
        document.field("user", session.getUserName());
        document.field("database", session.getDatabaseName());
        document.field("host", session.getParameter("host"));
        document.field("port", session.getParameter("port"));
        iResponse.writeResult(document, "indent:6", null);
      } catch (Exception e) {
        iResponse.send(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, e, null);
      }
      return false;
    }

  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
