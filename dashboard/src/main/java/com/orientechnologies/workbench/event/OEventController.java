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

public class OEventController {

	private static OEventController instance;
	private Map<String, Map<String, OEventExecutor>> executors = new HashMap<String, Map<String, OEventExecutor>>();

	public static OEventController getInstance() {
		if (instance == null) {
			instance = new OEventController();
		}
		return instance;
	}

	public OEventExecutor getExecutor(String when, String what) {
		return executors.get(when).get(what);
	}

	public void register(OEventExecutor e) {
		EventConfig a = e.getClass().getAnnotation(EventConfig.class);
		String when = a.when();
		String what = a.what();
		if (executors.get(when) == null) {
			executors.put(when, new HashMap<String, OEventExecutor>());
		}
		executors.get(when).put(what, e);

	}
}
