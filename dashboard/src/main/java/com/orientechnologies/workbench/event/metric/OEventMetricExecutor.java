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
package com.orientechnologies.workbench.event.metric;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.workbench.event.OEventExecutor;

public abstract class OEventMetricExecutor implements OEventExecutor {
	protected Map<String, Object>	body2name	= new HashMap<String, Object>();

	public boolean canExecute(ODocument source, ODocument when) {

		String whenMetricName = when.field("name");
		String whenOperator = when.field("operator");
		String whenParameter = when.field("parameter");
		Double whenValue = when.field("value"); // Integer

		String metricName = source.field("name");

		String metricValue = source.field(whenParameter);
		Double metricValueD = new Double(metricValue);
		if (metricName.equalsIgnoreCase(whenMetricName)) {

			if (whenOperator.equalsIgnoreCase("Greater Then") && metricValueD >= whenValue) {
				return true;
			}
			if (whenOperator.equalsIgnoreCase("Less Then") && metricValueD <= whenValue) {
				return true;
			}
		}

		return false;

	}

	protected void fillMapResolve(ODocument source, ODocument when) {

		this.getBody2name().clear();

		ODocument snapshot = source.field("snapshot");
		if (snapshot != null) {
			ODocument server = snapshot.field("server");
			if (server != null) {
				String serverName = server.field("name");
				this.getBody2name().put("servername", serverName);
			}
			Date dateFrom = snapshot.field("dateFrom");
			this.getBody2name().put("date", dateFrom);
		}
		String metricName = source.field("name");
		this.getBody2name().put("metric", metricName);

		String whenParameter = when.field("parameter");
		this.getBody2name().put("parameter", whenParameter);
		String metricValue = source.field(whenParameter);
		this.getBody2name().put("metricvalue", metricValue);

	}

	public Map<String, Object> getBody2name() {
		return body2name;
	}

	public void setBody2name(Map<String, Object> body2name) {
		this.body2name = body2name;
	}
}
