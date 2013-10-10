package com.orientechnologies.orient.monitor.http;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.monitor.OMonitorPlugin;
import com.orientechnologies.orient.monitor.OMonitorUtils;
import com.orientechnologies.orient.monitor.OMonitoredServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public class OServerCommandGetServerConfiguration extends
		OServerCommandAuthenticatedDbAbstract {

	private static final String[] NAMES = { "GET|configuration/*",
			"PUT|configuration/*" };
	private OMonitorPlugin monitor;

	public OServerCommandGetServerConfiguration(
			final OServerCommandConfiguration iConfiguration) {
	}

	public OServerCommandGetServerConfiguration() {
	}

	@Override
	public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse)
			throws Exception {

		final String[] urlParts = checkSyntax(iRequest.url, 3,
				"Syntax error: configuration/<db>/<server>");

		String server = urlParts[2];
		if (monitor == null)
			monitor = OServerMain.server().getPluginByClass(
					OMonitorPlugin.class);

		OMonitoredServer s = monitor.getMonitoredServer(server);
		ODocument serverConfig = s.getConfiguration();
		if (iRequest.httpMethod.equals("GET"))
			doGet(iResponse, serverConfig);
		if (iRequest.httpMethod.equals("PUT"))
			doPUT(iRequest, iResponse, serverConfig);
		return false;
	}

	private void doPUT(OHttpRequest iRequest, OHttpResponse iResponse,
			ODocument serverConfig) throws IOException {
		final URL remoteUrl = new java.net.URL("http://"
				+ serverConfig.field("url") + "/configuration/");

		ODocument doc = new ODocument();
		try {
			String response = OMonitorUtils.sendToRemoteServer(serverConfig,
					remoteUrl, "PUT", iRequest.content);
			doc.field("status", response);
		} catch (IOException e) {
			e.printStackTrace();
		}
		iResponse.writeRecord(doc);
	}

	private void doGet(OHttpResponse iResponse, ODocument serverConfig)
			throws MalformedURLException, IOException {
		final URL remoteUrl = new java.net.URL("http://"
				+ serverConfig.field("url") + "/configuration/");

		String response = OMonitorUtils.fetchFromRemoteServer(serverConfig,
				remoteUrl);

		ODocument doc = new ODocument();
		doc.field("configuration", response);
		iResponse.writeRecord(doc);
	}

	@Override
	public String[] getNames() {
		// TODO Auto-generated method stub
		return NAMES;
	}

}
