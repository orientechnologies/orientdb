package com.orientechnologies.orient.core.intent;

import com.orientechnologies.orient.core.db.ODatabase;

public interface OIntent {
	public void activate(ODatabase iDatabase, Object... iArgs);
}
