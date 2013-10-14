package com.orientechnologies.orient.monitor.event.metric;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.monitor.event.OEventExecutor;

public abstract class OEventMetricExecutor implements OEventExecutor {

	public boolean canExecute(ODocument source, ODocument when) {

		return false;
	}
}
