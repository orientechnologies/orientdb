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
import com.orientechnologies.workbench.*;

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
    ODatabaseDocumentTx databaseDocumentTx = getProfiledDatabaseInstance(iRequest);
    ODatabaseRecordThreadLocal.INSTANCE.set(databaseDocumentTx);
    if ("configuration".equals(type)) {
      List<ODocument> docs = new ArrayList<ODocument>();
      for (OMonitoredCluster cluster : monitor.getClustersList()) {
        docs.add(cluster.getClusterConfig());
      }
      iResponse.writeResult(docs);
    } else if ("dbconfig".equals(type)) {
      String cluster = urlParts[3];
      String db = urlParts[4];
      OMonitoredCluster c = monitor.getClusterByName(cluster);
      IMap<String, Object> config = c.getConfigurationMap();

      Collection<OMonitoredServer> servers = monitor.getServersByClusterName(cluster);
      ODocument dbConf = (ODocument) config.get("database." + db);
      Iterator<OMonitoredServer> iterator = servers.iterator();

      if (dbConf == null) {
        throw new RuntimeException("Cannot find database config in the cluster. Please check your configuration.");
      }
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
          e.printStackTrace();
        }
      }

      iResponse.writeResult(dbConf);
    } else if ("deploy".equals(type)) {
      String cluster = urlParts[3];
      String server = urlParts[4];
      String db = urlParts[5];

      OMonitoredCluster c = monitor.getClusterByName(cluster);

      OMonitoredServer s = monitor.getMonitoredServer(server);
      ODocument serverCfg = s.getConfiguration();

      try {
        final URL remoteUrl = new URL("http://" + serverCfg.field("url") + "/deployDb/" + db);
        String response = OWorkbenchUtils.fetchFromRemoteServer(serverCfg, remoteUrl);
        iResponse.send(OHttpUtils.STATUS_OK_CODE, null, null, response, null);
      } catch (IOException e) {
        throw e;
      }
    } else if ("disconnect".equals(type)) {
      String cluster = urlParts[3];
      OMonitoredCluster c = monitor.getClusterByName(cluster);
      if (c != null) {

        c.shutdownDistributed();
      }
      iResponse.send(OHttpUtils.STATUS_OK_CODE, null, null, null, null);
    } else if ("connect".equals(type)) {
      String cluster = urlParts[3];
      OMonitoredCluster c = monitor.getClusterByName(cluster);

      c.reInit();
      iResponse.send(OHttpUtils.STATUS_OK_CODE, null, null, null, null);
    }
  }

  private void doPost(OHttpRequest iRequest, OHttpResponse iResponse, String[] urlParts) throws InterruptedException, IOException {
    String type = urlParts[2];
    if ("configuration".equals(type)) {

      try {

        monitor.pauseClusterInspection();
        ODatabaseDocumentTx db = getProfiledDatabaseInstance(iRequest);
        ODatabaseRecordThreadLocal.INSTANCE.set(db);
        ODocument doc = new ODocument().fromJSON(iRequest.content, NO_MAP);
        OMonitoredCluster c = monitor.getClusterByName((String) doc.field("name"));

        String pwd = doc.field("password");
        if (pwd != null) {
          try {
            doc.field("password", OL.encrypt(pwd));
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        String status = doc.field("status");
        if (status == null) {
          doc.field("status", "CONNECTED");
        }
        ODocument res = doc.save();
        if (c != null) {
          c.refreshConfig(res);
        }
        OMonitoredCluster monitoredCluster = null;
        try {

          monitoredCluster = new OMonitoredCluster(monitor, res);
          monitor.addCluster(monitoredCluster);

        } catch (Exception e) {
          if (e instanceof OClusterException) {
            res.delete();
            iResponse.send(OHttpUtils.STATUS_BADREQ_CODE, null, null, "Error joining cluster " + doc.field("name")
                + ". Please check your cluster configuration", null);
          } else {
            iResponse.send(OHttpUtils.STATUS_BADREQ_CODE, null, null, e, null);
          }
        }
        iResponse.writeResult(res);
      } finally {
        monitor.resumeClusterInspection();
      }

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
