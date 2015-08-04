package com.orientechnologies.workbench.http;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import com.orientechnologies.workbench.OMonitoredServer;
import com.orientechnologies.workbench.OWorkbenchPlugin;
import com.orientechnologies.workbench.OWorkbenchUtils;

import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;

public class OServerCommandQueryPassThrough extends OServerCommandAuthenticatedDbAbstract {

  private static final String[] NAMES = { "GET|passThrough/*", "POST|passThrough/*" };
  private OWorkbenchPlugin      monitor;

  public OServerCommandQueryPassThrough() {
  }

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] parts = checkSyntax(iRequest.url, 2, "Syntax error: passThrough/monitor/<server>/<command>");

    if (monitor == null)
      monitor = OServerMain.server().getPluginByClass(OWorkbenchPlugin.class);

    StringWriter jsonBuffer = new StringWriter();
    if (parts[3].equals("alive")) {
      ODatabaseDocumentTx db = getProfiledDatabaseInstance(iRequest);
      ODatabaseRecordThreadLocal.INSTANCE.set(db);
      ODocument server = new ODocument();
      server.fromJSON(iRequest.content);
      final URL remoteUrl = new java.net.URL("http://" + server.field("url") + "/server");
      String response = OWorkbenchUtils.fetchFromRemoteServer(server, remoteUrl);
      ODocument doc = new ODocument().fromJSON(response);
      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, response, null);
    } else if (parts[3].equals("backup")) {
      OMonitoredServer serverM = monitor.getMonitoredServer(parts[2]);
      ODocument server = serverM.getConfiguration();
      final URL remoteUrl = new java.net.URL("http://" + server.field("url") + "/backup/" + parts[4]);
      InputStream response = OWorkbenchUtils.fetchInputRemoteServer(server, remoteUrl, "GET");
      iResponse.sendStream(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, OHttpUtils.CONTENT_GZIP, response, -1,
          parts[4] + ".gz");

    } else {
      iResponse.send(OHttpUtils.STATUS_NOTFOUND_CODE, OHttpUtils.STATUS_NOTFOUND_DESCRIPTION, OHttpUtils.CONTENT_JSON,
          jsonBuffer.toString(), null);
    }
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }

}
