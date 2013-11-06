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
package com.orientechnologies.orient.monitor;

import java.util.List;
import java.util.TimerTask;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

public final class OMonitorPurgeTask extends TimerTask {

	private final OMonitorPlugin handler;

	OMonitorPurgeTask(final OMonitorPlugin iHandler) {
		this.handler = iHandler;

	}

	@Override
	public void run() {
		try {

			OLogManager.instance().info(this,
					"MONITOR contacting workbench database...");

			String osql = "select from UserConfiguration where user.name = 'admin' ";

			OSQLQuery<ORecordSchemaAware<?>> osqlQuery = new OSQLSynchQuery<ORecordSchemaAware<?>>(
					osql);

			final List<ODocument> response = this.handler.getDb().query(
					osqlQuery);

			if (response.size() > 0) {
				ODocument userConfiguration = response.get(0);
				ODocument deleteConfiguration = userConfiguration
						.field("deleteMetricConfiguration");
				if (deleteConfiguration != null) {
					Integer hours = deleteConfiguration.field("hours");
					if (hours != null) {
						OMonitorPurgeMetricHelper.delete(hours,
								this.handler.getDb());
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}