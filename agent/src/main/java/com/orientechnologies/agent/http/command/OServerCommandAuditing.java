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

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.security.OSecurityServerUser;

import java.io.IOException;
import java.util.Collection;

public class OServerCommandAuditing extends OServerCommandDistributedScope {
  private static final String[] NAMES = { "GET|auditing/*", "POST|auditing/*" };
  private OEnterpriseAgent      oEnterpriseAgent;

  public OServerCommandAuditing(OEnterpriseAgent oEnterpriseAgent) {
    super("server.profiler");
    this.oEnterpriseAgent = oEnterpriseAgent;
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] parts = checkSyntax(iRequest.getUrl(), 3, "Syntax error: auditing/<db>/<action>");

    iRequest.data.commandInfo = "Auditing information";

    String db = parts[1];
    String action = parts[2];

    if (isLocalNode(iRequest)) {
      if ("GET".equals(iRequest.httpMethod)) {
        if (action.equalsIgnoreCase("config")) {
          doGet(iRequest, iResponse, parts[1]);
        }
      } else if ("POST".equals(iRequest.httpMethod)) {
        if (action.equalsIgnoreCase("config")) {
          doPost(iRequest, iResponse, db);
        } else if (action.equalsIgnoreCase("query")) {
          doGetData(iRequest, iResponse, db);

        }
      }
    } else {
      proxyRequest(iRequest, iResponse);
    }
    return false;
  }

  private void doGetData(OHttpRequest iRequest, OHttpResponse iResponse, String db) throws IOException, InterruptedException {

    ODocument params = new ODocument().fromJSON(iRequest.content);
    ODatabaseDocumentTx databaseDocumentTx = openAndSet(db);
    Collection<ODocument> documents = databaseDocumentTx.command(new OSQLSynchQuery<ODocument>(buildQuery(params))).execute(
        params.toMap());
    iResponse.writeResult(documents);
  }

  protected ODatabaseDocumentTx openAndSet(String dbName) {
    String s = server.getAvailableStorageNames().get(dbName);
    ODatabaseDocumentTx databaseDocumentTx = new ODatabaseDocumentTx(s);
    databaseDocumentTx.setProperty(ODatabase.OPTIONS.SECURITY.toString(), OSecurityServerUser.class);
    databaseDocumentTx.open("root", "nopassword");

    ODatabaseRecordThreadLocal.INSTANCE.set(databaseDocumentTx);

    return databaseDocumentTx;
  }

  private String buildQuery(ODocument params) {
    String query = "select from :clazz limit :limit";

    String clazz = params.field("clazz");
    Integer limit = params.field("limit");

    query = query.replace(":clazz", clazz);

    query = query.replace(":limit", "" + limit);
    return query;
  }

  private void doPost(OHttpRequest iRequest, OHttpResponse iResponse, String db) throws Exception {

    ODocument config = new ODocument().fromJSON(iRequest.content, "noMap");
    openAndSet(db);
    oEnterpriseAgent.auditingListener.changeConfig(db, config);
    iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, config.toJSON("prettyPrint=true"), null);
  }

  private void doGet(OHttpRequest iRequest, OHttpResponse iResponse, String db) throws Exception {

    ODocument config = oEnterpriseAgent.auditingListener.getConfig(db);
    iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, config.toJSON("prettyPrint=true"), null);

  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
