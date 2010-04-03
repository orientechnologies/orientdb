package com.orientechnologies.orient.core.intent;

import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;

public class OIntentMassiveRead implements OIntent {

	public void activate(ODatabaseRaw iDatabase, final Object... iArgs) {
		iDatabase.setUseCache(false);
	}
}
