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
package com.orientechnologies.workbench.event;

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.plugin.mail.OMailPlugin;
import com.orientechnologies.orient.server.plugin.mail.OMailProfile;
import com.orientechnologies.workbench.event.metric.OEventLogExecutor;

@EventConfig(when = "LogWhen", what = "MailWhat")
public class OEventLogMailExecutor extends OEventLogExecutor {
	Map<String, Object> body2name = new HashMap<String, Object>();
	private ODocument oUserConfiguration;
	private OMailPlugin mailPlugin;

	public OEventLogMailExecutor(ODatabaseDocumentTx database) {
		
		this.oUserConfiguration = EventHelper.findOrCreateMailUserConfiguration(database);
	}

	@Override
	public void execute(ODocument source, ODocument when, ODocument what) {
		ODocument server = when.field("server");
		this.body2name.clear();

		if (server != null) {
			String serverName = server.field("name");
			this.body2name.put("server", serverName);
		}

		String sourcelevel = (String) source.field("levelDescription");
		this.body2name.put("Log Value", sourcelevel);

		// pre-conditions
		if (canExecute(source, when)) {
			mailEvent(what);
		}
	}

	public void mailEvent(ODocument what) {
		if (mailPlugin == null) {

			mailPlugin = OServerMain.server().getPluginByClass(
					OMailPlugin.class);

			OMailProfile enterpriseProfile = EventHelper
					.createOMailProfile(oUserConfiguration);

			mailPlugin.registerProfile("enterprise", enterpriseProfile);
		}

		Map<String, Object> configuration = EventHelper.createConfiguration(
				what, body2name);

		try {
			mailPlugin.send(configuration);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
