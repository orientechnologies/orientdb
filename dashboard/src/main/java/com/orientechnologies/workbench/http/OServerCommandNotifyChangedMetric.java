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
package com.orientechnologies.workbench.http;

import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import com.orientechnologies.workbench.OWorkbenchPlugin;

public class OServerCommandNotifyChangedMetric extends OServerCommandAuthenticatedDbAbstract {

	private OWorkbenchPlugin				monitor;
	private static final String[]	NAMES	= { "POST|notifymetrics/*" };

	public OServerCommandNotifyChangedMetric(final OServerCommandConfiguration iConfiguration) {
	}

	public OServerCommandNotifyChangedMetric() {
	}

	@Override
	public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {

		if (monitor == null)
			monitor = OServerMain.server().getPluginByClass(OWorkbenchPlugin.class);

		final String[] urlParts = checkSyntax(iRequest.url, 1, "Syntax error: notifymetrics/");
		return false;
	}

	@Override
	public String[] getNames() {
		return NAMES;
	}

}
