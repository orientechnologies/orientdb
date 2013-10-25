package com.orientechnologies.orient.monitor.http;

import java.io.StringWriter;
import java.net.URL;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.monitor.OMonitorUtils;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public class OServerCommandQueryPassThrough extends OServerCommandAuthenticatedDbAbstract{

	private static final String[] NAMES = { "GET|passThrough/*","POST|passThrough/*" };
	
	public OServerCommandQueryPassThrough(final OServerCommandConfiguration iConfiguration) {
	}
	@Override
	public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse)
			throws Exception {
		final String[] parts = checkSyntax(iRequest.url, 2, "Syntax error: passThrough/monitor/<server>/<command>");
		
		
		StringWriter jsonBuffer = new StringWriter();
		if(parts[3].equals("alive")){
			
			ODocument server = new ODocument();
			server.fromJSON(iRequest.content);
			final URL remoteUrl = new java.net.URL("http://" + server.field("url") + "/server");
			String response =  OMonitorUtils.fetchFromRemoteServer(server, remoteUrl);
			iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, response, null);
		}else {
			iResponse.send(OHttpUtils.STATUS_NOTFOUND_CODE, OHttpUtils.STATUS_NOTFOUND_DESCRIPTION, OHttpUtils.CONTENT_JSON, jsonBuffer.toString(), null);	
		}
		return false;
	}

	@Override
	public String[] getNames() {
		return NAMES;
	}

}
