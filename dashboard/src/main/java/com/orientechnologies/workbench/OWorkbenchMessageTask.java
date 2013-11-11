package com.orientechnologies.workbench;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimerTask;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

public class OWorkbenchMessageTask extends TimerTask {

	private final OWorkbenchPlugin	handler;

	public OWorkbenchMessageTask(final OWorkbenchPlugin iHandler) {
		this.handler = iHandler;

	}

	@Override
	public void run() {

		String osql = "select from UserConfiguration where user.name = 'admin' ";

		OSQLQuery<ORecordSchemaAware<?>> osqlQuery = new OSQLSynchQuery<ORecordSchemaAware<?>>(osql);

		int cId = -1;
		String licenses = "";
		int i = 0;
		for (Entry<String, OMonitoredServer> server : this.handler.getMonitoredServers()) {
			ODocument s = server.getValue().getConfiguration();
			Map<String, Object> cfg = s.field("configuration");
			if (cfg != null) {

				String license = (String) cfg.get("license");
				cId = OL.getClientId(license);
				licenses += (i == 0) ? "" : ",";
				licenses += license;
				i++;
			}
		}

		final List<ODocument> response = this.handler.getDb().query(osqlQuery);
		licenses = licenses.isEmpty() ? "none" : licenses;
		if (response.size() > 0) {
			ODocument config = response.iterator().next();
			String url = "http://www.orientechnologies.com/"; // config.field("orientdbSite");
			//OLogManager.instance().info(this, "MONITOR contacting [%s] ", url);
			if (url != null) {
				try {
					URL remoteUrl = new java.net.URL(url + "pro/function/business/check/" + cId + "/" + licenses + "/true");
					URLConnection urlConnection = null;

					ODocument proxy = config.field("proxyConfiguration");
					String ip = proxy.field("proxyIp");
					Integer port = proxy.field("proxyPort");
					if (ip != null && port != null) {
						Proxy proxyConn = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(ip, port));
						urlConnection = remoteUrl.openConnection(proxyConn);
					} else {
						urlConnection = remoteUrl.openConnection();
					}
					urlConnection.connect();
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
						Map<String, Object> params = new HashMap<String, Object>();
						params.put("message", text);
						params.put("date", date);
						List<ODocument> resultSet = handler.getDb().query(new OSQLSynchQuery<ORecordSchemaAware<?>>("select from Message where message = :message and date = :date"), params);
						if (resultSet.isEmpty()) {
							ODocument saved = new ODocument("Message");
							saved.field("message", text);
							saved.field("date", date);
							saved.save();
						}
					}
				} catch (Exception e) {
					OLogManager.instance().error(this, "MONITOR error contacting [%s] ", url);
				}
			}
		}
	}
}
