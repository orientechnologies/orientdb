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
package com.orientechnologies.orient.monitor.event.metric;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.monitor.event.OEventExecutor;

public abstract class OEventMetricExecutor implements OEventExecutor {

	public boolean canExecute(ODocument source, ODocument when) {

		String whenMetricName = when.field("name");
		String whenOperator = when.field("operator");
		String whenParameter = when.field("parameter");
		Double whenValue = when.field("value"); // Integer

		String metricName = source.field("name");

		String metricValue = source.field(whenParameter);
		Double metricValueD = new Double(metricValue);
		if (metricName.equalsIgnoreCase(whenMetricName)) {

			if (whenOperator.equalsIgnoreCase("Greater Then")
					&& metricValueD >= whenValue) {
				return true;
			}
			if (whenOperator.equalsIgnoreCase("Less Then")
					&& metricValueD <= whenValue) {
				return true;
			}
		}
		
		return false;

	}
}
