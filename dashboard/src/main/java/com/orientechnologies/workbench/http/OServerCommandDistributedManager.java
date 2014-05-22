package com.orientechnologies.workbench.http;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import com.orientechnologies.workbench.OMonitoredCluster;
import com.orientechnologies.workbench.OWorkbenchPlugin;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by enricorisa on 21/05/14.
 */
public class OServerCommandDistributedManager extends OServerCommandAuthenticatedDbAbstract {
  private static final String[] NAMES = { "GET|distributed/*", "POST|distributed/*" };
  private OWorkbenchPlugin      monitor;

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] urlParts = checkSyntax(iRequest.url, 2, "Syntax error: distributed/<db>/<type>");

    if (monitor == null)
      monitor = OServerMain.server().getPluginByClass(OWorkbenchPlugin.class);

    if (iRequest.httpMethod.equals("POST")) {

      ODatabaseDocumentTx db = getProfiledDatabaseInstance(iRequest);
      ODatabaseRecordThreadLocal.INSTANCE.set(db);
      ODocument doc = new ODocument().fromJSON(iRequest.content);
      iResponse.writeResult(doc.save());
    } else {
      if (urlParts.length > 2) {
        String type = urlParts[2];
        if ("configuration".equals(type)) {
          List<ODocument> docs = new ArrayList<ODocument>();
          for (OMonitoredCluster cluster : monitor.getClustersList()) {
            docs.add(cluster.getClusterConfig());
          }
          iResponse.writeResult(docs);
        }

      }
    }
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
