package com.orientechnologies.workbench;

import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimerTask;

public class OWorkbenchMessageTask extends TimerTask {

  private final OWorkbenchPlugin handler;

  public OWorkbenchMessageTask(final OWorkbenchPlugin iHandler) {
    this.handler = iHandler;

  }

  @Override
  public void run() {

    String osql = "select from UserConfiguration where user.name = 'admin' ";

    OSQLQuery<ORecordSchemaAware> osqlQuery = new OSQLSynchQuery<ORecordSchemaAware>(osql);

    int cId = -1;
    int i = 0;

    ODocument body = new ODocument();
    body.field("workbenchVersion", handler.version);
    Map<String, String> agents = new HashMap<String, String>();
    for (Entry<String, OMonitoredServer> server : this.handler.getMonitoredServers()) {
      ODocument s = server.getValue().getConfiguration();

//      Map<String, Object> cfg = s.field("configuration");
//      if (cfg != null) {
//        String license = (String) cfg.get("license");
//        String version = (String) cfg.get("agentVersion");
//        cId = OL.getClientId(license);
//        agents.put(license, version);
//      }
    }
    body.field("agents", agents);

    final List<ODocument> response = this.handler.getDb().query(osqlQuery);
    if (response.size() > 0) {
      ODocument config = response.iterator().next();
      String url = config.field("orientdbSite"); // "http://www.orientechnologies.com/";
      // OLogManager.instance().info(this, "MONITOR contacting [%s] ", url);
      if (url != null) {
        try {
          ODocument updateConfiguration = config.field("updateConfiguration");
          Boolean receiveNews = (Boolean) (updateConfiguration != null ? updateConfiguration.field("receiveNews") : true);
          URL remoteUrl = new java.net.URL(url + "pro/function/business/check/" + cId + "/" + receiveNews);
          HttpURLConnection urlConnection = null;

          ODocument proxy = config.field("proxyConfiguration");
          if (proxy != null) {
            String ip = proxy.field("proxyIp");
            Integer port = proxy.field("proxyPort");
            if (ip != null && port != null) {
              Proxy proxyConn = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(ip, port));
              urlConnection = (HttpURLConnection) remoteUrl.openConnection(proxyConn);
            } else {
              urlConnection = (HttpURLConnection) remoteUrl.openConnection();
            }
          } else {
            urlConnection = (HttpURLConnection) remoteUrl.openConnection();
          }
          urlConnection.setDoOutput(true);
          urlConnection.setRequestMethod("POST");
          urlConnection.connect();

          OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream());
          out.write(body.toJSON());
          out.close();
          InputStream is = urlConnection.getInputStream();
          InputStreamReader isr = new InputStreamReader(is);

          int numCharsRead;
          char[] charArray = new char[1024];
          StringBuffer sb = new StringBuffer();
          while ((numCharsRead = isr.read(charArray)) > 0) {
            sb.append(charArray, 0, numCharsRead);
          }
          String result = sb.toString();

          final ODocument docMetrics = new ODocument().fromJSON(result);

          List<Map<String, Object>> results = docMetrics.field("result");
          Map<String, Object> map = results.get(0);
          Object obj = map.get("messages");
          List<Map<String, Object>> oDocument = (List<Map<String, Object>>) map.get("messages");
          for (Map<String, Object> values : oDocument) {

            Object text = values.get("text");
            Object date = values.get("date");
            Object subject = values.get("subject");
            Object type = values.get("type");
            Object payload = values.get("payload");

            Map<String, Object> params = new HashMap<String, Object>();
            params.put("message", text);
            params.put("date", date);
            List<ODocument> resultSet = handler.getDb().query(
                new OSQLSynchQuery<ORecordSchemaAware>("select from Message where message = :message and date = :date"), params);
            if (resultSet.isEmpty()) {
              ODocument saved = new ODocument("Message");
              saved.field("message", text);
              saved.field("date", date);
              saved.field("subject", subject);
              saved.field("type", type);
              saved.field("payload", payload);

              saved.save();
            }
          }
        } catch (Exception e) {

        }
      }
    }
  }
}
