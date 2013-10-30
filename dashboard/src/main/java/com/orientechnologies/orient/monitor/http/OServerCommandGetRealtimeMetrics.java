/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.monitor.http;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.monitor.OMonitorPlugin;
import com.orientechnologies.orient.monitor.OMonitoredServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public class OServerCommandGetRealtimeMetrics extends OServerCommandAuthenticatedDbAbstract {
	private static final String[]	NAMES	= { "GET|metrics/*" };

	private OMonitorPlugin				monitor;

	public OServerCommandGetRealtimeMetrics(final OServerCommandConfiguration iConfiguration) {
	}

	@Override
	public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
		if (monitor == null)
			monitor = OServerMain.server().getPluginByClass(OMonitorPlugin.class);

		final String[] parts = checkSyntax(iRequest.url, 7,
				"Syntax error: metrics/monitor/<server>/<databases>/<type>/<kind>/<names>/<limit>/<compress>/<from>/<to>");

		iRequest.data.commandInfo = "Retrieve metrics";

		try {

			final String serverName = parts[2];
			final String[] databases = parts[3].split(",");
			final String type = parts[4];
			final String metricKind = parts[5];
			final String[] metricNames = parts[6].split(",");
			String from = null;
			String to = null;
			String compress = null;
			String limit = null;
			if (parts.length > 7) {
				limit = parts[7];
			}
			if (parts.length > 8) {
				compress = parts[8];
			}
			if (parts.length > 9) {
				from = parts[9];
			}
			if (parts.length > 10) {
				to = parts[10];
			}
			final OMonitoredServer server = monitor.getMonitoredServer(serverName);
			if (server == null)
				throw new IllegalArgumentException("Invalid server '" + serverName + "'");

			final Map<String, Object> result = new HashMap<String, Object>();

			if ("realtime".equalsIgnoreCase(type))
				sendRealtimeMetrics(iResponse, metricKind, metricNames, server, databases, result);
			else if ("snapshot".equalsIgnoreCase(type)) {
				sendSnapshotMetrics(iRequest, iResponse, metricKind, metricNames, server, databases, limit, compress, from, to, result);
			}

		} catch (Exception e) {
			iResponse.send(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, e, null);
		}
		return false;
	}

	protected void sendRealtimeMetrics(OHttpResponse iResponse, final String iMetricKind, final String[] metricNames,
			final OMonitoredServer server, String[] databases, final Map<String, Object> result) throws MalformedURLException,
			IOException, InterruptedException {
		String[] dbs = databases;
		if (databases.length == 1 && databases[0].equals("all")) {
			try {
				final Map<String, Object> mapDb = server.getRealtime().getInformation("system.databases");
				String dbInfo = (String) mapDb.get("system.databases");
				dbs = dbInfo.split(",");
			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (IOException e) {

			}

		}
		Map<String, String> aggregation = buildAssociation(server, metricNames, dbs);
		for (String metricName : expandMetric(server, metricNames, dbs)) {
			final Map<String, Object> metrics;

			if (iMetricKind.equals("chrono"))
				metrics = server.getRealtime().getChrono(metricName);
			else if (iMetricKind.equals("statistics"))
				metrics = server.getRealtime().getStatistic(metricName);
			else if (iMetricKind.equals("information"))
				metrics = server.getRealtime().getInformation(metricName);
			else if (iMetricKind.equals("counter"))
				metrics = server.getRealtime().getCounter(metricName);
			else
				throw new IllegalArgumentException("Unsupported type '" + iMetricKind + "'");

			if (metrics != null)
				for (Entry<String, Object> metric : metrics.entrySet()) {
					String key = aggregation.get(metric.getKey());
					if (key == null) {
						key = metric.getKey();
					}
					result.put(key, metric.getValue());
				}
		}

		iResponse.writeResult(result, "indent:6");
	}

	protected void sendSnapshotMetrics(OHttpRequest iRequest, OHttpResponse iResponse, final String iMetricKind,
			final String[] metricNames, final OMonitoredServer server, String[] databases, String limit, String compress, String from,
			String to, final Map<String, Object> result) throws MalformedURLException, IOException, InterruptedException {
		String query = "select @class, snapshot.dateTo as dateTo,snapshot.dateFrom as dateFrom, name, entries, last, min, max, average,value,total from Metric where name in :names and snapshot.server.name = :sname ";
		String[] dbs = databases;
		if (databases.length == 1 && databases[0].equals("all")) {
			try {
				final Map<String, Object> mapDb = server.getRealtime().getInformation("system.databases");
				String dbInfo = (String) mapDb.get("system.databases");
				dbs = dbInfo.split(",");
			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (IOException e) {

			}

		}
		final Map<String, Object> params = new HashMap<String, Object>();
		params.put("names", expandMetric(server, metricNames, dbs));
		params.put("sname", server.getConfiguration().field("name"));

		 if (from != null) {
			query += "and snapshot.dateFrom >= :dateFrom ";
			params.put("dateFrom", from);
		}
		if (to != null) {
			query += "and snapshot.dateTo <= :dateTo";
			params.put("dateTo", to);
		}
		query += " order by dateFrom desc, name desc ";
		if (limit != null) {
			query += " LIMIT " + limit;
		}

		List<ODocument> docs = getProfiledDatabaseInstance(iRequest).query(new OSQLSynchQuery<ORecordSchemaAware<?>>(query), params);

		Map<String, String> aggregation = buildAssociation(server, metricNames, dbs);

		for (ODocument oDocument : docs) {
			oDocument.field("name", aggregation.get(oDocument.field("name")));
		}
		iResponse.writeResult(docs);
	}

	protected Map<String, String> buildAssociation(OMonitoredServer server, String[] metrics, String[] databases) {
		Map<String, String> ass = new HashMap<String, String>();
		String[] dbFormatted;
		dbFormatted = databases;
		for (String m : metrics) {
			if (m.startsWith("db")) {
				for (String db : dbFormatted) {
					String replace = m.replace("*", db);
					ass.put(replace, m);
				}
			} else {
				ass.put(m, m);
			}
		}
		return ass;
	}

	protected List<String> expandMetric(OMonitoredServer server, String[] metrics, String[] databases) {
		String[] dbFormatted;
		List<String> finalMetrics = new ArrayList<String>();
		if (databases.length == 0)
			return Arrays.asList(metrics);

		dbFormatted = databases;

		for (String m : metrics) {
			if (m.startsWith("db")) {
				for (String db : dbFormatted) {
					finalMetrics.add(m.replace("*", db));
				}
			} else {
				finalMetrics.add(m);
			}
		}

		return finalMetrics;
	}

	@Override
	public String[] getNames() {
		return NAMES;
	}
}
