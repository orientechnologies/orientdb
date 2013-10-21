package com.orientechnologies.orient.monitor.event;

import com.orientechnologies.orient.core.record.impl.ODocument;

public interface OEventExecutor {

	public void execute(ODocument source,ODocument when, ODocument what);
}
