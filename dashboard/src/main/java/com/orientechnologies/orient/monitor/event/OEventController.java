package com.orientechnologies.orient.monitor.event;

import java.util.HashMap;
import java.util.Map;

public class OEventController {

	private static OEventController										instance;
	private Map<String, Map<String, OEventExecutor>>	executors = new HashMap<String, Map<String,OEventExecutor>>();

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
