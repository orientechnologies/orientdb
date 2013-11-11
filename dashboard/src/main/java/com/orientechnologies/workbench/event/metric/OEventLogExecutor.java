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

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.workbench.event.OEventExecutor;

public abstract class OEventLogExecutor implements OEventExecutor {

	public boolean canExecute(ODocument source, ODocument when) {

		String operator = when.field("alertValue");
		String levelType = when.field("type");
		String info = when.field("info");

		String sourcelevel = (String) source.field("levelDescription");
		String message = (String) source.field("description");

		if (operator == null && levelType == null && info == null) {
			return false;
		}

		if (info != null && !info.isEmpty()) {

			if (message == null || !message.contains(info)) {
				return false;
			}
		}
		if (levelType != null
				&& !levelType.isEmpty()) {
			// Integer level = new Integer(levelType);
			// Integer logLevel = new Integer(sourcelevel);

			if (levelType.equalsIgnoreCase(sourcelevel))
				return true;
			return false;
			// if (operator.equalsIgnoreCase("greater then")
			// && sourcelevel >= level) {
			// return true;
			// } else if (operator.equalsIgnoreCase("less then")
			// && sourcelevel <= level) {
			// return true;
			// }
			// return false;
		}
		return false;

	}
}
