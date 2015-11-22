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
package com.orientechnologies.agent.event;

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
