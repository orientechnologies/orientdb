package com.orientechnologies.orient.core.intent;

import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;

public class OIntentMassiveRead implements OIntent {
	private boolean	previousUseCache;

	public void begin(final ODatabaseRaw iDatabase, final Object... iArgs) {
		previousUseCache = iDatabase.isUseCache();

		iDatabase.setUseCache(true);
	}

	public void end(final ODatabaseRaw iDatabase) {
		iDatabase.setUseCache(previousUseCache);
	}
}
