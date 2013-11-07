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
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.monitor.event.metric.OEventLogExecutor;
import com.orientechnologies.orient.monitor.hooks.OEventHook;

@EventConfig(when = "LogWhen", what = "HttpWhat")
public class OEventLogHttpExecutor extends OEventLogExecutor {
	Map<String, Object>					body2name		= new HashMap<String, Object>();


	private ODatabaseDocumentTx	db;

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

		String sourcelevel = (String) source.field("levelDescription");
		this.body2name.put("Log Value", sourcelevel);

		// pre-conditions
		if (canExecute(source, when)) {
			try {
				executeHttp(what);
			} catch (MalformedURLException e) {
				e.printStackTrace();
			}
		}
	}

	private void executeHttp(ODocument what) throws MalformedURLException {

		
		EventHelper.executeHttpRequest(what,db);

	}
}
