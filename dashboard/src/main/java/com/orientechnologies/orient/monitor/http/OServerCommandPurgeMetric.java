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
package com.orientechnologies.orient.monitor.http;

import com.orientechnologies.orient.monitor.OMonitorPlugin;
import com.orientechnologies.orient.monitor.OMonitorPurgeMetricHelper;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public class OServerCommandPurgeMetric extends
		OServerCommandAuthenticatedDbAbstract {

	private OMonitorPlugin monitor;
	private static final String[] NAMES = { "GET|purge/*" };

	private static final String METRICS = "metrics";
	private static final String LOGS = "logs";

	public OServerCommandPurgeMetric(
			final OServerCommandConfiguration iConfiguration) {
	}

	public OServerCommandPurgeMetric() {
	}

	@Override
	public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse)
			throws Exception {

		if (monitor == null)
			monitor = OServerMain.server().getPluginByClass(
					OMonitorPlugin.class);

		final String[] urlParts = checkSyntax(iRequest.url, 1,
				"Syntax error: purge/metrics");

		if (METRICS.equalsIgnoreCase(urlParts[2])) {
			OMonitorPurgeMetricHelper
					.purgeMetricNow(getProfiledDatabaseInstance(iRequest));

			return true;
		}
		if (LOGS.equalsIgnoreCase(urlParts[2])) {
			OMonitorPurgeMetricHelper
					.purgeLogsNow(getProfiledDatabaseInstance(iRequest));

			return true;
		}
		return false;

	}

	@Override
	public String[] getNames() {
		return NAMES;
	}

}
