package com.orientechnologies.orient.core.intent;

import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;

public interface OIntent {
	public void activate(ODatabaseRaw iDatabase, Object... iArgs);
}
