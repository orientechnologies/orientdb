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

package com.orientechnologies.workbench.hooks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.hook.ORecordHookAbstract;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.workbench.event.OEventController;
import com.orientechnologies.workbench.event.OEventExecutor;

public class OEventHook extends ORecordHookAbstract {

	@Override
	public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void onRecordAfterCreate(ORecord<?> iiRecord) {

		ODocument doc = (ODocument) iiRecord;
		List<ODocument> triggers = new ArrayList<ODocument>();

		if (doc.getClassName().equalsIgnoreCase("Log")) {
			// String iText = "select from Event where when.type = '" + doc.field("levelDescription") + "'";
			String iText = "select from Event";
			ODocument serverLog = doc.field("server");
			String url = serverLog.field("url");
			String description = doc.field("description");
			triggers = doc.getDatabase().query(new OSQLSynchQuery<Object>(iText));
			for (ODocument oDocument : triggers) {

				ODocument when = oDocument.field("when");
				ODocument what = oDocument.field("what");
				String classWhen = when.field("@class");
				String classWhat = what.field("@class");
				OEventExecutor executor = OEventController.getInstance().getExecutor(classWhen, classWhat);

				ODocument serverWhen = when.field("server");
				String infoWhen = when.field("info");
				String whenType = when.field("type");
				if (whenType != null) {
					String logdescription = doc.field("levelDescription");
					if (!logdescription.equals(whenType)) {
						break;
					}
				}
				if (serverWhen != null) {
					String urlwhen = serverWhen.field("url");
					if (!url.equals(urlwhen)) {
						break;
					}
				}
				if (infoWhen != null) {
					if (!description.contains(infoWhen)) {
						break;
					}
				}
				executor.execute(doc, when, what);
			}
		} else {

			String metricName = doc.field("name");
			ODocument snapshot = doc.field("snapshot");
			ODocument server = (ODocument) (snapshot != null ? snapshot.field("server") : null);
			if (server != null) {
				String urlServer = server.field("url");
				if (metricName != null)
					urlServer = urlServer.split(":")[0];
				metricName = metricName.replaceAll(urlServer + ":" + "[0-9]*.", "*.");

			}
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("name", metricName);
			triggers = doc.getDatabase().query(new OSQLSynchQuery<Object>("select from Event where when.name = :name"));

			for (ODocument oDocument : triggers) {

				ODocument when = oDocument.field("when");
				ODocument what = oDocument.field("what");
				String classWhen = when.field("@class");
				String classWhat = what.field("@class");
				OEventExecutor executor = OEventController.getInstance().getExecutor(classWhen, classWhat);
				executor.execute(doc, when, what);

			}
		}

	}
}
