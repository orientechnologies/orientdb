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

import java.net.MalformedURLException;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.workbench.event.metric.OEventMetricExecutor;

@EventConfig(when = "MetricsWhen", what = "FunctionWhat")
public class OEventMetricHttpExecutor extends OEventMetricExecutor {
	private ODatabaseDocumentTx	db;

	public OEventMetricHttpExecutor(ODatabaseDocumentTx database) {

		this.db = database;
	}

	@Override
	public void execute(ODocument source, ODocument when, ODocument what) {
	
		// pre-conditions
		if (canExecute(source, when)) {
			fillMapResolve(source, when);
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
