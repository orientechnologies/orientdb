package com.orientechnologies.workbench.http;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

/**
 * Created by enricorisa on 21/05/14.
 */
public class OServerCommandClusterManager extends OServerCommandAuthenticatedDbAbstract {
  private static final String[] NAMES = { "GET|cluster/*", "POST|cluster/*" };

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] urlParts = checkSyntax(iRequest.url, 2, "Syntax error: loggedUserInfo/<db>/<type>");

    if (iRequest.httpMethod.equals("POST")) {

      ODatabaseDocumentTx db = getProfiledDatabaseInstance(iRequest);
      ODatabaseRecordThreadLocal.INSTANCE.set(db);
      ODocument doc = new ODocument().fromJSON(iRequest.content);
      iResponse.writeResult(doc.save());
    } else {

    }
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
