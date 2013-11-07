/*
 * Copyright 2010-2013 Orient Technologies LTD
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
 */
package com.orientechnologies.orient.monitor.event;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.ConnectionReuseStrategy;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.RequestExpectContinue;
import org.apache.http.impl.DefaultBHttpClientConnection;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.HttpProcessorBuilder;
import org.apache.http.protocol.HttpRequestExecutor;
import org.apache.http.protocol.RequestConnControl;
import org.apache.http.protocol.RequestContent;
import org.apache.http.protocol.RequestTargetHost;
import org.apache.http.protocol.RequestUserAgent;
import org.apache.http.util.EntityUtils;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.monitor.event.metric.OEventLogExecutor;

@EventConfig(when = "LogWhen", what = "HttpWhat")
public class OEventLogHttpExecutor extends OEventLogExecutor {
	Map<String, Object> body2name = new HashMap<String, Object>();

	private final String USER_AGENT = "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.69 Safari/537.36";

	private ODatabaseDocumentTx db;

	public OEventLogHttpExecutor(ODatabaseDocumentTx database) {

		this.db = database;
	}

	@Override
	public void execute(ODocument source, ODocument when, ODocument what) {

		ODocument server = when.field("server");
		this.body2name.clear();

		if (server != null) {
			this.body2name.put("server", server);

		}
		String metricName = source.field("name");
		this.body2name.put("metric", metricName);
		
		String sourcelevel = (String) source.field("level");
		this.body2name.put("Log Value", sourcelevel);

		// pre-conditions
		if (canExecute(source, when)) {
			try {
				executeHttp(what);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (HttpException e) {
				e.printStackTrace();
			}
		}
	}

	private void executeHttp(ODocument what) throws IOException, HttpException {
		String url = what.field("url");

		String method = what.field("method"); // GET POST

		Integer port = what.field("port");

		String body = what.field("body");

		HttpHost host = new HttpHost(url, port, "http");

		HttpRequestExecutor httpexecutor = new HttpRequestExecutor();
		BasicHttpRequest request = new BasicHttpRequest(method, "/archivio");
		HttpCoreContext coreContext = HttpCoreContext.create();
		coreContext.setTargetHost(host);

		DefaultBHttpClientConnection conn = new DefaultBHttpClientConnection(
				8 * 1024);
		ConnectionReuseStrategy connStrategy = DefaultConnectionReuseStrategy.INSTANCE;

		try {

			Socket socket = new Socket("it.eurosport.yahoo.com", 8080);
			conn.bind(socket);
			System.out.println(">> Request URI: "
					+ request.getRequestLine().getUri());

			HttpResponse response = httpexecutor.execute(request, conn,
					coreContext);

			System.out.println("<< Response: " + response.getStatusLine());
			System.out.println(EntityUtils.toString(response.getEntity()));
			System.out.println("==============");
			if (!connStrategy.keepAlive(response, coreContext)) {
				conn.close();
			} else {
				System.out.println("Connection kept alive...");
			}
		} catch (Exception e) {

			e.printStackTrace();
		} finally {
			conn.close();
		}
	}

}
