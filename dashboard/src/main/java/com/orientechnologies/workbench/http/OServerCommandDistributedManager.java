package com.orientechnologies.workbench.http;

import com.hazelcast.core.IMap;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import com.orientechnologies.workbench.OMonitoredCluster;
import com.orientechnologies.workbench.OMonitoredServer;
import com.orientechnologies.workbench.OWorkbenchPlugin;
import com.orientechnologies.workbench.OWorkbenchUtils;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Created by enricorisa on 21/05/14.
 */
public class OServerCommandDistributedManager extends OServerCommandAuthenticatedDbAbstract {
  private static final String[] NAMES  = { "GET|distributed/*", "POST|distributed/*" };
  public static final String    NO_MAP = "noMap";
  private OWorkbenchPlugin      monitor;

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] urlParts = checkSyntax(iRequest.url, 2, "Syntax error: distributed/<db>/<type>/<cluster>");

    if (monitor == null)
      monitor = OServerMain.server().getPluginByClass(OWorkbenchPlugin.class);

    if (iRequest.httpMethod.equals("POST")) {

      if (urlParts.length > 2) {
        String type = urlParts[2];
        if ("configuration".equals(type)) {
          ODatabaseDocumentTx db = getProfiledDatabaseInstance(iRequest);
          ODatabaseRecordThreadLocal.INSTANCE.set(db);
          ODocument doc = new ODocument().fromJSON(iRequest.content, NO_MAP);
          iResponse.writeResult(doc.save());
        } else if ("dbconfig".equals(type)) {
          String cluster = urlParts[3];
          String db = urlParts[4];
          ODocument doc = new ODocument().fromJSON(iRequest.content, NO_MAP);
          OMonitoredCluster c = monitor.getClusterByName(cluster);
          IMap<String, Object> config = c.getConfigurationMap();
          config.put("database." + db, doc);
          iResponse.send(OHttpUtils.STATUS_OK_CODE, null, null, OHttpUtils.STATUS_OK_DESCRIPTION, null);
        }
      }

    } else {
      if (urlParts.length > 2) {
        String type = urlParts[2];
        if ("configuration".equals(type)) {
          List<ODocument> docs = new ArrayList<ODocument>();
          for (OMonitoredCluster cluster : monitor.getClustersList()) {
            docs.add(cluster.getClusterConfig());
          }
          iResponse.writeResult(docs);
        } else if ("dbconfig".equals(type)) {
          String cluster = urlParts[3];
          String db = urlParts[4];
          ODatabaseDocumentTx databaseDocumentTx = getProfiledDatabaseInstance(iRequest);
          ODatabaseRecordThreadLocal.INSTANCE.set(databaseDocumentTx);
          OMonitoredCluster c = monitor.getClusterByName(cluster);
          IMap<String, Object> config = c.getConfigurationMap();

          Collection<OMonitoredServer> servers = monitor.getServersByClusterName(cluster);
          ODocument dbConf = (ODocument) config.get("database." + db);
          Iterator<OMonitoredServer> iterator = servers.iterator();
          while (iterator.hasNext()) {
            ODocument server = iterator.next().getConfiguration();
            try {
              final URL remoteUrl = new java.net.URL("http://" + server.field("url") + "/database/" + db);
              String response = OWorkbenchUtils.fetchFromRemoteServer(server, remoteUrl);
              ODocument result = new ODocument();
              result.fromJSON(response);
              dbConf.field("metadata", result);
              break;
            } catch (Exception e) {

            }
          }

          iResponse.writeResult(dbConf);
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
