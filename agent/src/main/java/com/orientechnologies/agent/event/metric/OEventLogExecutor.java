/*
 * Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */
package com.orientechnologies.agent.event.metric;

import com.orientechnologies.agent.event.OEventExecutor;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public abstract class OEventLogExecutor implements OEventExecutor {
	protected Map<String, Object>	body2name	= new HashMap<String, Object>();

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
		if (levelType != null && !levelType.isEmpty()) {
			if (levelType.equalsIgnoreCase(sourcelevel))
				return true;
			return false;
		}
		return false;

	}

	public Map<String, Object> getBody2name() {
		return body2name;
	}

	public void setBody2name(Map<String, Object> body2name) {
		this.body2name = body2name;
	}

	protected void fillMapResolve(ODocument source, ODocument when) {
		this.body2name.clear();
		ODocument server = source.field("server");
		this.getBody2name().clear();
		Date date = source.field("date");
		this.getBody2name().put("date", date);
		if (server != null) {
			String serverName = server.field("name");
			this.getBody2name().put("servername", serverName);

		}
		String metricName = source.field("name");
		this.getBody2name().put("metric", metricName);

		String sourcelevel = (String) source.field("levelDescription");
		this.getBody2name().put("logvalue", sourcelevel);
	}
}
