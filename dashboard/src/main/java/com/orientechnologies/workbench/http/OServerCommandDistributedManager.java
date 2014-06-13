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

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Created by enricorisa on 21/05/14.
 */
public class OServerCommandDistributedManager extends OServerCommandAuthenticatedDbAbstract {
  private static final String[] NAMES  = { "GET|distributed/*", "POST|distributed/*", "DELETE|distributed/*" };
  public static final String    NO_MAP = "noMap";
  private OWorkbenchPlugin      monitor;

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] urlParts = checkSyntax(iRequest.url, 2, "Syntax error: distributed/<db>/<type>/<cluster>");

    if (monitor == null)
      monitor = OServerMain.server().getPluginByClass(OWorkbenchPlugin.class);

    if (iRequest.httpMethod.equals("POST")) {

      if (urlParts.length > 2) {
        doPost(iRequest, iResponse, urlParts);
      }

    } else if (iRequest.httpMethod.equals("GET")) {
      if (urlParts.length > 2) {
        doGet(iRequest, iResponse, urlParts);

      }
    } else if (iRequest.httpMethod.equals("DELETE")) {
      doDelete(iRequest, iResponse, urlParts);
    }
    return false;
  }

  private void doDelete(OHttpRequest iRequest, OHttpResponse iResponse, String[] urlParts) throws InterruptedException, IOException {
    String type = urlParts[2];
    if ("configuration".equals(type)) {
      String cluster = urlParts[3];
      OMonitoredCluster c = monitor.getClusterByName(cluster);
      ODatabaseDocumentTx databaseDocumentTx = getProfiledDatabaseInstance(iRequest);
      ODatabaseRecordThreadLocal.INSTANCE.set(databaseDocumentTx);
      c.getClusterConfig().delete();
      monitor.removeMonitoredCluster(cluster);
      c.shutdownDistributed();
      iResponse.send(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, null, null);
    }
  }

  private void doGet(OHttpRequest iRequest, OHttpResponse iResponse, String[] urlParts) throws InterruptedException, IOException {
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
          final URL remoteUrl = new URL("http://" + server.field("url") + "/database/" + db);
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

  private void doPost(OHttpRequest iRequest, OHttpResponse iResponse, String[] urlParts) throws InterruptedException, IOException {
    String type = urlParts[2];
    if ("configuration".equals(type)) {
      ODatabaseDocumentTx db = getProfiledDatabaseInstance(iRequest);
      ODatabaseRecordThreadLocal.INSTANCE.set(db);
      ODocument doc = new ODocument().fromJSON(iRequest.content, NO_MAP);
      OMonitoredCluster c = monitor.getClusterByName((String) doc.field("name"));

      ODocument res = doc.save();
      if (c != null) {
        c.refreshConfig(res);
      }
      iResponse.writeResult(res);
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

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
